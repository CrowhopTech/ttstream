package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/mock"
)

// newMockServer builds a Server whose redisClient is a mockery-generated
// mock, so tests can stub individual Get/Set calls per key.
func newMockServer(t *testing.T) (*Server, *MockRedisClient) {
	t.Helper()
	m := NewMockRedisClient(t)
	cfg := &config{
		RedisStatusOutputName: "statuses:openai_text_generator",
		QwenTTSSpeakerKey:     "statuses:qwen_tts_speaker",
		TTSVoiceOptionsKey:    "options:tts_voices",
		PromptOptionsKey:      "options:prompts",
		LastHeartbeatKey:      "session:last_heartbeat",
		WebpageKeepaliveKey:   "session:webpage_connected",
		SessionIDKey:          "session:id",
		SessionInfoKey:        "session:info",
		KeepaliveTTL:          5 * time.Second,
	}
	srv := &Server{
		redisClient:          m,
		keepaliveKey:         cfg.WebpageKeepaliveKey,
		keepaliveCheckTicker: time.NewTicker(time.Second),
		canceled:             make(chan struct{}),
		cfg:                  cfg,
	}
	t.Cleanup(func() {
		srv.keepaliveCheckTicker.Stop()
	})
	return srv, m
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

// ---------------------------------------------------------------------------
// handleWebpageHeartbeat: exact ordering of SET failures
// ---------------------------------------------------------------------------

func TestHandleWebpageHeartbeat_KeepaliveSetFails_500(t *testing.T) {
	srv, m := newMockServer(t)

	// Heartbeat SET succeeds, keepalive SET fails.
	m.EXPECT().
		Set(mock.Anything, srv.cfg.LastHeartbeatKey, mock.Anything, mock.Anything).
		Return(statusCmdOK()).Once()
	m.EXPECT().
		Set(mock.Anything, srv.cfg.WebpageKeepaliveKey, mock.Anything, mock.Anything).
		Return(statusCmdErr(errors.New("keepalive boom"))).Once()

	req := httptest.NewRequest(http.MethodPost, "/webpage-keepalive", nil)
	w := httptest.NewRecorder()
	srv.handleWebpageHeartbeat(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d want 500; body=%s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "failed to write webpage keepalive") {
		t.Errorf("body=%q missing expected error text", w.Body.String())
	}
}

func TestHandleWebpageHeartbeat_HeartbeatSetFails_500(t *testing.T) {
	srv, m := newMockServer(t)

	// Only the heartbeat SET is expected — handler must bail before the second.
	m.EXPECT().
		Set(mock.Anything, srv.cfg.LastHeartbeatKey, mock.Anything, mock.Anything).
		Return(statusCmdErr(errors.New("heartbeat boom"))).Once()

	req := httptest.NewRequest(http.MethodPost, "/webpage-keepalive", nil)
	w := httptest.NewRecorder()
	srv.handleWebpageHeartbeat(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d want 500", w.Code)
	}
	if !strings.Contains(w.Body.String(), "failed to write heartbeat") {
		t.Errorf("body=%q missing expected text", w.Body.String())
	}
	// Assertions are checked automatically at t.Cleanup via NewMockRedisClient(t):
	// if the keepalive SET had been invoked, testify would fail the test for
	// an unexpected call.
}

// ---------------------------------------------------------------------------
// handleUpdateSession: exact ordering of SET failures
// ---------------------------------------------------------------------------

func TestHandleUpdateSession_SessionInfoSetFails_500(t *testing.T) {
	srv, m := newMockServer(t)

	m.EXPECT().
		Set(mock.Anything, srv.cfg.SessionIDKey, "p", mock.Anything).
		Return(statusCmdOK()).Once()
	m.EXPECT().
		Set(mock.Anything, srv.cfg.SessionInfoKey, mock.Anything, mock.Anything).
		Return(statusCmdErr(errors.New("info boom"))).Once()

	req := httptest.NewRequest(http.MethodPost, "/update_session?voice=v&prompt=p", nil)
	w := httptest.NewRecorder()
	srv.handleUpdateSession(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d want 500", w.Code)
	}
}

func TestHandleUpdateSession_MarshalFails_500(t *testing.T) {
	srv, _ := newMockServer(t)

	// Force json.Marshal to fail. Restore in cleanup so other tests are
	// unaffected. No Redis calls are expected — the handler bails before
	// touching Redis.
	orig := jsonMarshal
	jsonMarshal = func(any) ([]byte, error) {
		return nil, errors.New("marshal boom")
	}
	t.Cleanup(func() { jsonMarshal = orig })

	req := httptest.NewRequest(http.MethodPost, "/update_session?voice=v&prompt=p", nil)
	w := httptest.NewRecorder()
	srv.handleUpdateSession(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d want 500", w.Code)
	}
	if !strings.Contains(w.Body.String(), "Marshal error") {
		t.Errorf("body=%q missing 'Marshal error'", w.Body.String())
	}
}

func TestHandleUpdateSession_SessionIDSetFails_500(t *testing.T) {
	srv, m := newMockServer(t)

	// Only the SessionID SET is expected.
	m.EXPECT().
		Set(mock.Anything, srv.cfg.SessionIDKey, "p", mock.Anything).
		Return(statusCmdErr(errors.New("id boom"))).Once()

	req := httptest.NewRequest(http.MethodPost, "/update_session?voice=v&prompt=p", nil)
	w := httptest.NewRecorder()
	srv.handleUpdateSession(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d want 500", w.Code)
	}
}

// ---------------------------------------------------------------------------
// handleWebpageStatus: mixed per-key GET errors
// ---------------------------------------------------------------------------

func TestHandleWebpageStatus_OpenAIGetFails_QwenValid(t *testing.T) {
	srv, m := newMockServer(t)

	m.EXPECT().
		Get(mock.Anything, srv.cfg.RedisStatusOutputName).
		Return(stringCmdErr(errors.New("openai down"))).Once()
	m.EXPECT().
		Get(mock.Anything, srv.cfg.QwenTTSSpeakerKey).
		Return(stringCmdVal(`{"status":"idle","as_of":42}`)).Once()

	req := httptest.NewRequest(http.MethodGet, "/webpage_status.json", nil)
	w := httptest.NewRecorder()
	srv.handleWebpageStatus(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status=%d want 200", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, `"status":"error"`) {
		t.Errorf("body=%q expected openai error status", body)
	}
	if !strings.Contains(body, `"status":"idle"`) {
		t.Errorf("body=%q expected qwen idle status", body)
	}
}

func TestHandleWebpageStatus_QwenGetFails_OpenAIValid(t *testing.T) {
	srv, m := newMockServer(t)

	m.EXPECT().
		Get(mock.Anything, srv.cfg.RedisStatusOutputName).
		Return(stringCmdVal(`{"status":"ok","as_of":42}`)).Once()
	m.EXPECT().
		Get(mock.Anything, srv.cfg.QwenTTSSpeakerKey).
		Return(stringCmdErr(errors.New("qwen down"))).Once()

	req := httptest.NewRequest(http.MethodGet, "/webpage_status.json", nil)
	w := httptest.NewRecorder()
	srv.handleWebpageStatus(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status=%d want 200", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, `"status":"ok"`) {
		t.Errorf("body=%q expected openai ok", body)
	}
	if !strings.Contains(body, `"status":"error"`) {
		t.Errorf("body=%q expected qwen error", body)
	}
}

// ---------------------------------------------------------------------------
// handleSpeechOptions: mixed per-key GET errors
// ---------------------------------------------------------------------------

func TestHandleSpeechOptions_VoicesFail_PromptsValid(t *testing.T) {
	srv, m := newMockServer(t)

	m.EXPECT().
		Get(mock.Anything, srv.cfg.TTSVoiceOptionsKey).
		Return(stringCmdErr(errors.New("voices down"))).Once()
	m.EXPECT().
		Get(mock.Anything, srv.cfg.PromptOptionsKey).
		Return(stringCmdVal(`["p"]`)).Once()

	req := httptest.NewRequest(http.MethodGet, "/speech_options", nil)
	w := httptest.NewRecorder()
	srv.handleSpeechOptions(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status=%d want 200; body=%s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), `"prompts":["p"]`) {
		t.Errorf("body=%q missing prompts", w.Body.String())
	}
}

func TestHandleSpeechOptions_BothFail_ForcedErrors_500(t *testing.T) {
	srv, m := newMockServer(t)

	m.EXPECT().
		Get(mock.Anything, srv.cfg.TTSVoiceOptionsKey).
		Return(stringCmdErr(errors.New("everything down"))).Once()
	m.EXPECT().
		Get(mock.Anything, srv.cfg.PromptOptionsKey).
		Return(stringCmdErr(errors.New("everything down"))).Once()

	req := httptest.NewRequest(http.MethodGet, "/speech_options", nil)
	w := httptest.NewRecorder()
	srv.handleSpeechOptions(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d want 500", w.Code)
	}
}

// ---------------------------------------------------------------------------
// checkKeepalive: SET-error branch on keepalive key
// ---------------------------------------------------------------------------

func TestCheckKeepalive_Mock_SetErrorOnKeepaliveKey(t *testing.T) {
	srv, m := newMockServer(t)

	// Expired heartbeat, then SET returns an error.
	m.EXPECT().
		Get(mock.Anything, srv.cfg.LastHeartbeatKey).
		Return(stringCmdVal(fmt.Sprintf("%d", time.Now().Add(-time.Hour).Unix()))).Once()
	m.EXPECT().
		Set(mock.Anything, srv.cfg.WebpageKeepaliveKey, mock.Anything, mock.Anything).
		Return(statusCmdErr(errors.New("write failed"))).Once()

	// Must not panic — the function only logs on set errors.
	srv.checkKeepalive(context.Background(), 5*time.Second)
}

func TestCheckKeepalive_Mock_ContextCanceledDuringSet(t *testing.T) {
	srv, m := newMockServer(t)

	m.EXPECT().
		Get(mock.Anything, srv.cfg.LastHeartbeatKey).
		Return(stringCmdVal(fmt.Sprintf("%d", time.Now().Add(-time.Hour).Unix()))).Once()
	m.EXPECT().
		Set(mock.Anything, srv.cfg.WebpageKeepaliveKey, mock.Anything, mock.Anything).
		Return(statusCmdErr(context.Canceled)).Once()

	// Ensure the context.Canceled branch suppresses the log without panicking.
	srv.checkKeepalive(context.Background(), 5*time.Second)
}
