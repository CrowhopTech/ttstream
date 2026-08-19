package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/mock"
)

// newMockConfig builds a config with sensible defaults for mock-based tests.
func newMockConfig() *config {
	return &config{
		Model:                 "test-model",
		RedisStatusOutputName: "statuses:openai_text_generator",
		RedisTriggerKeyName:   "session:webpage_connected",
		RedisPromptsKeyName:   "options:prompts",
		SessionIDKey:          "session:id",
		SessionInfoKey:        "session:info",
		OutputQueueNamePrefix: "queues:generated_text",
		MaxQueueLength:        10,
	}
}

// stringCmdVal builds a *redis.StringCmd carrying the given value.
func stringCmdVal(v string) *redis.StringCmd {
	c := redis.NewStringCmd(context.Background(), "get")
	c.SetVal(v)
	return c
}

// stringCmdErr builds a *redis.StringCmd carrying the given error.
func stringCmdErr(err error) *redis.StringCmd {
	c := redis.NewStringCmd(context.Background(), "get")
	c.SetErr(err)
	return c
}

// statusCmdOK builds a successful *redis.StatusCmd.
func statusCmdOK() *redis.StatusCmd {
	c := redis.NewStatusCmd(context.Background(), "set")
	c.SetVal("OK")
	return c
}

// statusCmdErr builds a failing *redis.StatusCmd.
func statusCmdErr(err error) *redis.StatusCmd {
	c := redis.NewStatusCmd(context.Background(), "set")
	c.SetErr(err)
	return c
}

// intCmdVal builds a *redis.IntCmd carrying the given value.
func intCmdVal(v int64) *redis.IntCmd {
	c := redis.NewIntCmd(context.Background(), "int")
	c.SetVal(v)
	return c
}

// intCmdErr builds a *redis.IntCmd carrying the given error.
func intCmdErr(err error) *redis.IntCmd {
	c := redis.NewIntCmd(context.Background(), "int")
	c.SetErr(err)
	return c
}

// ---------------------------------------------------------------------------
// setStatus: exact key & payload assertions
// ---------------------------------------------------------------------------

func TestSetStatus_Mock_WritesToConfiguredKey(t *testing.T) {
	m := NewMockRedisClient(t)
	cfg := newMockConfig()

	m.EXPECT().
		Set(mock.Anything, cfg.RedisStatusOutputName, mock.Anything, time.Duration(0)).
		Return(statusCmdOK()).Once()

	setStatus(context.Background(), m, cfg, "starting up")
}

func TestSetStatus_Mock_PanicsOnSetErr(t *testing.T) {
	m := NewMockRedisClient(t)
	cfg := newMockConfig()

	m.EXPECT().
		Set(mock.Anything, cfg.RedisStatusOutputName, mock.Anything, time.Duration(0)).
		Return(statusCmdErr(errors.New("set failed"))).Once()

	defer func() {
		if recover() == nil {
			t.Fatal("expected setStatus to panic when Set errors")
		}
	}()
	setStatus(context.Background(), m, cfg, "will panic")
}

// ---------------------------------------------------------------------------
// getSessionIDAndInfo: mixed per-key GET behaviour
// ---------------------------------------------------------------------------

func TestGetSessionIDAndInfo_Mock_HappyPath(t *testing.T) {
	m := NewMockRedisClient(t)
	cfg := newMockConfig()

	m.EXPECT().
		Get(mock.Anything, cfg.SessionIDKey).
		Return(stringCmdVal("sid-1")).Once()
	m.EXPECT().
		Get(mock.Anything, cfg.SessionInfoKey).
		Return(stringCmdVal(`{"as_of":99,"voice_id":"v","prompt_id":"p"}`)).Once()

	sid, info, err := getSessionIDAndInfo(context.Background(), m, cfg)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if sid != "sid-1" {
		t.Errorf("sid=%q", sid)
	}
	if info.AsOf != 99 || info.VoiceID != "v" || info.PromptID != "p" {
		t.Errorf("info=%+v", info)
	}
}

func TestGetSessionIDAndInfo_Mock_SessionIDGetFails(t *testing.T) {
	m := NewMockRedisClient(t)
	cfg := newMockConfig()

	// Only the SessionID GET is expected — handler must bail before the second.
	m.EXPECT().
		Get(mock.Anything, cfg.SessionIDKey).
		Return(stringCmdErr(errors.New("id down"))).Once()

	_, _, err := getSessionIDAndInfo(context.Background(), m, cfg)
	if err == nil {
		t.Fatal("want error")
	}
}

func TestGetSessionIDAndInfo_Mock_SessionInfoGetFails(t *testing.T) {
	m := NewMockRedisClient(t)
	cfg := newMockConfig()

	m.EXPECT().
		Get(mock.Anything, cfg.SessionIDKey).
		Return(stringCmdVal("sid")).Once()
	m.EXPECT().
		Get(mock.Anything, cfg.SessionInfoKey).
		Return(stringCmdErr(errors.New("info down"))).Once()

	_, _, err := getSessionIDAndInfo(context.Background(), m, cfg)
	if err == nil {
		t.Fatal("want error")
	}
}

// ---------------------------------------------------------------------------
// loadPromptsToRedis: interaction with a mocked Redis client
// ---------------------------------------------------------------------------

func TestLoadPromptsToRedis_Mock_EmptyDirWritesEmptyArrayString(t *testing.T) {
	m := NewMockRedisClient(t)
	cfg := newMockConfig()

	dir := t.TempDir()
	withPromptsDir(t, dir)

	// Empty dir → the "[]" literal branch.
	m.EXPECT().
		Set(mock.Anything, cfg.RedisPromptsKeyName, "[]", time.Duration(0)).
		Return(statusCmdOK()).Once()

	got, err := loadPromptsToRedis(context.Background(), m, cfg)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if len(got) != 0 {
		t.Errorf("got=%v want empty", got)
	}
}

func TestLoadPromptsToRedis_Mock_NonEmptyWritesJSONBytes(t *testing.T) {
	m := NewMockRedisClient(t)
	cfg := newMockConfig()

	dir := t.TempDir()
	withPromptsDir(t, dir)
	writeFile(t, dir, "one.txt", "hi")

	// Non-empty → the []byte JSON branch. Match any payload.
	m.EXPECT().
		Set(mock.Anything, cfg.RedisPromptsKeyName, mock.Anything, time.Duration(0)).
		Return(statusCmdOK()).Once()

	got, err := loadPromptsToRedis(context.Background(), m, cfg)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if len(got) != 1 || got[0] != "one" {
		t.Errorf("got=%v", got)
	}
}

// ---------------------------------------------------------------------------
// waitForSessionInit: exercises Exists() branches
// ---------------------------------------------------------------------------

func TestWaitForSessionInit_Mock_KeyExistsImmediately(t *testing.T) {
	m := NewMockRedisClient(t)
	cfg := newMockConfig()

	m.EXPECT().
		Exists(mock.Anything, cfg.SessionIDKey).
		Return(intCmdVal(1)).Once()

	if err := waitForSessionInit(context.Background(), m, cfg); err != nil {
		t.Fatalf("err=%v", err)
	}
}

func TestWaitForSessionInit_Mock_ExistsError_ReturnsErr(t *testing.T) {
	m := NewMockRedisClient(t)
	cfg := newMockConfig()

	m.EXPECT().
		Exists(mock.Anything, cfg.SessionIDKey).
		Return(intCmdErr(errors.New("exists down"))).Once()

	err := waitForSessionInit(context.Background(), m, cfg)
	if err == nil {
		t.Fatal("want error")
	}
}

func TestWaitForSessionInit_Mock_MissingThenPresent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping slow test in -short mode")
	}
	m := NewMockRedisClient(t)
	cfg := newMockConfig()

	// First call: missing (returns 0). Second call: present (returns 1).
	m.EXPECT().
		Exists(mock.Anything, cfg.SessionIDKey).
		Return(intCmdVal(0)).Once()
	m.EXPECT().
		Exists(mock.Anything, cfg.SessionIDKey).
		Return(intCmdVal(1)).Once()

	done := make(chan error, 1)
	go func() { done <- waitForSessionInit(context.Background(), m, cfg) }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("err=%v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("did not converge within 3s")
	}
}

func TestWaitForSessionInit_Mock_ContextCanceledMidLoop(t *testing.T) {
	m := NewMockRedisClient(t)
	cfg := newMockConfig()

	// Keep returning "missing"; expect handler to observe ctx cancel and bail.
	m.EXPECT().
		Exists(mock.Anything, cfg.SessionIDKey).
		Return(intCmdVal(0)).Maybe()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	done := make(chan error, 1)
	go func() { done <- waitForSessionInit(ctx, m, cfg) }()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("want error on canceled context")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("did not exit within 3s")
	}
}
