package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// newTestConfig returns a config populated with sensible defaults matching
// the real envDefaults, pointed at the given miniredis instance.
func newTestConfig(t *testing.T, mr *miniredis.Miniredis) *config {
	t.Helper()
	return &config{
		Model:                 "test-model",
		LlamaServer:           "localhost",
		LlamaPort:             8080,
		RedisAddress:          mr.Host(),
		RedisPort:             mustAtoi(t, mr.Port()),
		RedisStatusOutputName: "statuses:openai_text_generator",
		RedisTriggerKeyName:   "session:webpage_connected",
		RedisPromptsKeyName:   "options:prompts",
		SessionIDKey:          "session:id",
		SessionInfoKey:        "session:info",
		OutputQueueNamePrefix: "queues:generated_text",
		MaxQueueLength:        10,
	}
}

func newRedisClient(cfg *config) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("%s:%d", cfg.RedisAddress, cfg.RedisPort),
	})
}

func mustAtoi(t *testing.T, s string) int {
	t.Helper()
	var n int
	if _, err := fmt.Sscanf(s, "%d", &n); err != nil {
		t.Fatalf("bad int %q: %v", s, err)
	}
	return n
}

// withPromptsDir overrides the package-level getPromptsDir to return the given
// path, restoring it on cleanup.
func withPromptsDir(t *testing.T, dir string) {
	t.Helper()
	orig := getPromptsDir
	getPromptsDir = func() string { return dir }
	t.Cleanup(func() { getPromptsDir = orig })
}

func writeFile(t *testing.T, dir, name, contents string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(contents), 0o644); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
}

// ---------------------------------------------------------------------------
// NewPunctuationChunker
// ---------------------------------------------------------------------------

func TestNewPunctuationChunker_StoresMin(t *testing.T) {
	c := NewPunctuationChunker(42)
	if c == nil {
		t.Fatal("NewPunctuationChunker returned nil")
	}
	if c.minCharsPerChunk != 42 {
		t.Errorf("minCharsPerChunk=%d want 42", c.minCharsPerChunk)
	}
}

func TestNewPunctuationChunker_ZeroMin(t *testing.T) {
	c := NewPunctuationChunker(0)
	if c.minCharsPerChunk != 0 {
		t.Errorf("minCharsPerChunk=%d want 0", c.minCharsPerChunk)
	}
}

// ---------------------------------------------------------------------------
// PunctuationChunker.PrecleanString
// ---------------------------------------------------------------------------

func TestPrecleanString_ReplacesEllipsisTriple(t *testing.T) {
	c := NewPunctuationChunker(1)
	got := c.PrecleanString("hello... world")
	want := "hello… world"
	if got != want {
		t.Errorf("got=%q want=%q", got, want)
	}
}

func TestPrecleanString_TrimsWhitespace(t *testing.T) {
	c := NewPunctuationChunker(1)
	got := c.PrecleanString("   padded   ")
	if got != "padded" {
		t.Errorf("got=%q want=%q", got, "padded")
	}
}

func TestPrecleanString_MultipleTripleDots(t *testing.T) {
	c := NewPunctuationChunker(1)
	got := c.PrecleanString("a...b...c")
	want := "a…b…c"
	if got != want {
		t.Errorf("got=%q want=%q", got, want)
	}
}

func TestPrecleanString_NoDotsUnchanged(t *testing.T) {
	c := NewPunctuationChunker(1)
	got := c.PrecleanString("nothing to change")
	if got != "nothing to change" {
		t.Errorf("got=%q", got)
	}
}

func TestPrecleanString_Empty(t *testing.T) {
	c := NewPunctuationChunker(1)
	if got := c.PrecleanString(""); got != "" {
		t.Errorf("got=%q want empty", got)
	}
}

func TestPrecleanString_TwoDotsUnchanged(t *testing.T) {
	// Only literal "..." is replaced.
	c := NewPunctuationChunker(1)
	got := c.PrecleanString("wait..stop")
	if got != "wait..stop" {
		t.Errorf("got=%q want unchanged", got)
	}
}

// ---------------------------------------------------------------------------
// PunctuationChunker.IsDelimiter
// ---------------------------------------------------------------------------

func TestIsDelimiter_TrueForKnown(t *testing.T) {
	c := NewPunctuationChunker(1)
	for _, r := range []rune{'.', '?', '!', '…'} {
		if !c.IsDelimiter(r) {
			t.Errorf("IsDelimiter(%q) = false, want true", r)
		}
	}
}

func TestIsDelimiter_FalseForOthers(t *testing.T) {
	c := NewPunctuationChunker(1)
	for _, r := range []rune{',', ';', ':', 'a', ' ', '\n', 0} {
		if c.IsDelimiter(r) {
			t.Errorf("IsDelimiter(%q) = true, want false", r)
		}
	}
}

// ---------------------------------------------------------------------------
// PunctuationChunker.ChunkString
// ---------------------------------------------------------------------------

func TestChunkString_Empty(t *testing.T) {
	c := NewPunctuationChunker(1)
	got := c.ChunkString("")
	if len(got) != 0 {
		t.Errorf("got=%v want empty", got)
	}
}

func TestChunkString_OnlyWhitespaceReturnsNoChunks(t *testing.T) {
	c := NewPunctuationChunker(1)
	got := c.ChunkString("     ")
	if len(got) != 0 {
		t.Errorf("got=%v want empty", got)
	}
}

func TestChunkString_NoDelimiter_SingleChunk(t *testing.T) {
	c := NewPunctuationChunker(1)
	got := c.ChunkString("just some text without terminators")
	want := []string{"just some text without terminators"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got=%v want=%v", got, want)
	}
}

func TestChunkString_DelimiterBelowMinCharsCoalesces(t *testing.T) {
	// minCharsPerChunk = 100. Two short sentences should coalesce into one
	// chunk (the trailing flush) since neither buffer ever reaches 100.
	c := NewPunctuationChunker(100)
	got := c.ChunkString("Hi. Bye.")
	want := []string{"Hi. Bye."}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got=%v want=%v", got, want)
	}
}

func TestChunkString_DelimiterAtOrAboveMin_Splits(t *testing.T) {
	c := NewPunctuationChunker(5)
	// "Hello." is 6 chars; splits at the '.'. "World!" is 7 chars including the
	// leading space that gets appended to the second buffer.
	got := c.ChunkString("Hello. World!")
	want := []string{"Hello.", " World!"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got=%v want=%v", got, want)
	}
}

func TestChunkString_TrailingTextWithoutDelimiterFlushes(t *testing.T) {
	c := NewPunctuationChunker(5)
	got := c.ChunkString("Hello. tail")
	// The trailing " tail" has no delimiter and gets flushed trimmed.
	if len(got) != 2 {
		t.Fatalf("got=%v want length 2", got)
	}
	if got[0] != "Hello." {
		t.Errorf("chunk[0]=%q want Hello.", got[0])
	}
	if got[1] != "tail" {
		t.Errorf("chunk[1]=%q want 'tail' (trimmed)", got[1])
	}
}

func TestChunkString_TripleDotBecomesEllipsisDelimiter(t *testing.T) {
	c := NewPunctuationChunker(3)
	got := c.ChunkString("wait...go")
	// "..." collapses to a single … rune; buffer must reach 3 runes to
	// split. "wait…" is 5 runes, so splits after ellipsis.
	if len(got) != 2 {
		t.Fatalf("got=%v want length 2", got)
	}
	if got[0] != "wait…" {
		t.Errorf("chunk[0]=%q", got[0])
	}
	if got[1] != "go" {
		t.Errorf("chunk[1]=%q", got[1])
	}
}

func TestChunkString_QuestionAndBang(t *testing.T) {
	c := NewPunctuationChunker(3)
	got := c.ChunkString("Why? Because!")
	// "Why?" = 4 runes → splits. " Because!" = 9 runes → splits.
	want := []string{"Why?", " Because!"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got=%v want=%v", got, want)
	}
}

func TestChunkString_ConsecutiveDelimitersWithinBuffer(t *testing.T) {
	c := NewPunctuationChunker(3)
	// After "abc." splits, next buffer starts at "." which is a delimiter but
	// buffer length is 1 (< 3), so it does not split, then accumulates.
	got := c.ChunkString("abc..d")
	// First split: "abc." (4 >= 3). Then buffer becomes ".d" - final flush,
	// trimmed = ".d".
	if len(got) != 2 {
		t.Fatalf("got=%v want length 2", got)
	}
	if got[0] != "abc." {
		t.Errorf("chunk[0]=%q want abc.", got[0])
	}
	if got[1] != ".d" {
		t.Errorf("chunk[1]=%q want .d", got[1])
	}
}

func TestChunkString_UnicodeRunesCounted(t *testing.T) {
	c := NewPunctuationChunker(4)
	// Non-ASCII characters count as one rune each. "héllo." is 6 runes, so it
	// splits at the '.'.
	got := c.ChunkString("héllo.")
	want := []string{"héllo."}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got=%v want=%v", got, want)
	}
}

func TestChunkString_PrecleaningTrimsBeforeChunking(t *testing.T) {
	c := NewPunctuationChunker(3)
	got := c.ChunkString("   Hi.   ")
	// leading/trailing whitespace stripped; then "Hi." (3 runes) splits.
	want := []string{"Hi."}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got=%v want=%v", got, want)
	}
}

// ---------------------------------------------------------------------------
// setStatus
// ---------------------------------------------------------------------------

func TestSetStatus_WritesJSONToRedis(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := newTestConfig(t, mr)
	r := newRedisClient(cfg)
	t.Cleanup(func() { _ = r.Close() })

	setStatus(context.Background(), r, cfg, "hello")

	raw, err := mr.Get(cfg.RedisStatusOutputName)
	if err != nil {
		t.Fatalf("get status: %v", err)
	}
	// The struct fields are unexported so they don't serialize; the payload is
	// just "{}". We're asserting the call didn't panic and wrote *something*
	// valid-JSON to the expected key.
	if !json.Valid([]byte(raw)) {
		t.Errorf("stored value is not valid JSON: %q", raw)
	}
}

func TestSetStatus_RedisDown_Panics(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := newTestConfig(t, mr)
	r := newRedisClient(cfg)
	t.Cleanup(func() { _ = r.Close() })

	mr.Close()

	defer func() {
		if recover() == nil {
			t.Fatal("expected setStatus to panic when Redis is unreachable")
		}
	}()
	setStatus(context.Background(), r, cfg, "goes nowhere")
}

// ---------------------------------------------------------------------------
// getOutputQueueName
// ---------------------------------------------------------------------------

func TestGetOutputQueueName_FormatsWithColon(t *testing.T) {
	cfg := &config{OutputQueueNamePrefix: "queues:generated_text"}
	if got := getOutputQueueName(cfg, "abc123"); got != "queues:generated_text:abc123" {
		t.Errorf("got=%q", got)
	}
}

func TestGetOutputQueueName_EmptySessionID(t *testing.T) {
	cfg := &config{OutputQueueNamePrefix: "queues:generated_text"}
	if got := getOutputQueueName(cfg, ""); got != "queues:generated_text:" {
		t.Errorf("got=%q", got)
	}
}

func TestGetOutputQueueName_EmptyPrefix(t *testing.T) {
	cfg := &config{OutputQueueNamePrefix: ""}
	if got := getOutputQueueName(cfg, "sid"); got != ":sid" {
		t.Errorf("got=%q", got)
	}
}

// ---------------------------------------------------------------------------
// getSessionIDAndInfo
// ---------------------------------------------------------------------------

func TestGetSessionIDAndInfo_HappyPath(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := newTestConfig(t, mr)
	r := newRedisClient(cfg)
	t.Cleanup(func() { _ = r.Close() })

	mr.Set(cfg.SessionIDKey, "session-42")
	mr.Set(cfg.SessionInfoKey, `{"as_of":123,"voice_id":"alice","prompt_id":"greet"}`)

	sid, info, err := getSessionIDAndInfo(context.Background(), r, cfg)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if sid != "session-42" {
		t.Errorf("sid=%q", sid)
	}
	if info.AsOf != 123 || info.VoiceID != "alice" || info.PromptID != "greet" {
		t.Errorf("info=%+v", info)
	}
}

func TestGetSessionIDAndInfo_MissingSessionIDKey(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := newTestConfig(t, mr)
	r := newRedisClient(cfg)
	t.Cleanup(func() { _ = r.Close() })

	_, _, err := getSessionIDAndInfo(context.Background(), r, cfg)
	if err == nil {
		t.Fatal("want error when session ID key missing")
	}
	if !strings.Contains(err.Error(), "failed to get session ID") {
		t.Errorf("err=%v missing expected text", err)
	}
}

func TestGetSessionIDAndInfo_EmptySessionID(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := newTestConfig(t, mr)
	r := newRedisClient(cfg)
	t.Cleanup(func() { _ = r.Close() })

	mr.Set(cfg.SessionIDKey, "   ")
	mr.Set(cfg.SessionInfoKey, `{"as_of":1,"voice_id":"v","prompt_id":"p"}`)

	_, _, err := getSessionIDAndInfo(context.Background(), r, cfg)
	if err == nil {
		t.Fatal("want error on whitespace-only session ID")
	}
	if !strings.Contains(err.Error(), "is empty") {
		t.Errorf("err=%v missing 'is empty'", err)
	}
}

func TestGetSessionIDAndInfo_MissingSessionInfoKey(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := newTestConfig(t, mr)
	r := newRedisClient(cfg)
	t.Cleanup(func() { _ = r.Close() })

	mr.Set(cfg.SessionIDKey, "sid")

	_, _, err := getSessionIDAndInfo(context.Background(), r, cfg)
	if err == nil {
		t.Fatal("want error when session info key missing")
	}
	if !strings.Contains(err.Error(), "failed to get session info") {
		t.Errorf("err=%v missing expected text", err)
	}
}

func TestGetSessionIDAndInfo_MalformedInfoJSON(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := newTestConfig(t, mr)
	r := newRedisClient(cfg)
	t.Cleanup(func() { _ = r.Close() })

	mr.Set(cfg.SessionIDKey, "sid")
	mr.Set(cfg.SessionInfoKey, "not-json")

	_, _, err := getSessionIDAndInfo(context.Background(), r, cfg)
	if err == nil {
		t.Fatal("want error on malformed JSON")
	}
	if !strings.Contains(err.Error(), "failed to parse session info") {
		t.Errorf("err=%v missing expected text", err)
	}
}

func TestGetSessionIDAndInfo_RedisDown_Error(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := newTestConfig(t, mr)
	r := newRedisClient(cfg)
	t.Cleanup(func() { _ = r.Close() })
	mr.Close()

	_, _, err := getSessionIDAndInfo(context.Background(), r, cfg)
	if err == nil {
		t.Fatal("want error when Redis is unreachable")
	}
}

// ---------------------------------------------------------------------------
// loadPromptText
// ---------------------------------------------------------------------------

func TestLoadPromptText_HappyPath(t *testing.T) {
	dir := t.TempDir()
	withPromptsDir(t, dir)
	writeFile(t, dir, "greet.txt", "Hello, world!")

	got, err := loadPromptText("greet")
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if got != "Hello, world!" {
		t.Errorf("got=%q", got)
	}
}

func TestLoadPromptText_MissingFile(t *testing.T) {
	dir := t.TempDir()
	withPromptsDir(t, dir)

	_, err := loadPromptText("nope")
	if err == nil {
		t.Fatal("want error for missing prompt file")
	}
	if !strings.Contains(err.Error(), "failed to read prompt contents") {
		t.Errorf("err=%v missing expected text", err)
	}
}

func TestLoadPromptText_EmptyFile(t *testing.T) {
	dir := t.TempDir()
	withPromptsDir(t, dir)
	writeFile(t, dir, "empty.txt", "")

	got, err := loadPromptText("empty")
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if got != "" {
		t.Errorf("got=%q want empty", got)
	}
}

// ---------------------------------------------------------------------------
// loadPromptsToRedis
// ---------------------------------------------------------------------------

func TestLoadPromptsToRedis_WritesTxtNamesWithoutExtension(t *testing.T) {
	dir := t.TempDir()
	withPromptsDir(t, dir)
	writeFile(t, dir, "greet.txt", "hi")
	writeFile(t, dir, "goodbye.txt", "bye")
	// Non-.txt files must be ignored.
	writeFile(t, dir, "readme.md", "ignored")

	mr := miniredis.RunT(t)
	cfg := newTestConfig(t, mr)
	r := newRedisClient(cfg)
	t.Cleanup(func() { _ = r.Close() })

	got, err := loadPromptsToRedis(context.Background(), r, cfg)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	want := map[string]bool{"greet": true, "goodbye": true}
	if len(got) != 2 {
		t.Fatalf("got=%v want 2 entries", got)
	}
	for _, name := range got {
		if !want[name] {
			t.Errorf("unexpected prompt %q", name)
		}
	}

	raw, err := mr.Get(cfg.RedisPromptsKeyName)
	if err != nil {
		t.Fatalf("get key: %v", err)
	}
	var stored []string
	if err := json.Unmarshal([]byte(raw), &stored); err != nil {
		t.Fatalf("unmarshal stored: %v", err)
	}
	if len(stored) != 2 {
		t.Errorf("stored=%v want length 2", stored)
	}
}

func TestLoadPromptsToRedis_EmptyDir_WritesEmptyArray(t *testing.T) {
	dir := t.TempDir()
	withPromptsDir(t, dir)

	mr := miniredis.RunT(t)
	cfg := newTestConfig(t, mr)
	r := newRedisClient(cfg)
	t.Cleanup(func() { _ = r.Close() })

	got, err := loadPromptsToRedis(context.Background(), r, cfg)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if len(got) != 0 {
		t.Errorf("got=%v want empty", got)
	}
	raw, err := mr.Get(cfg.RedisPromptsKeyName)
	if err != nil {
		t.Fatalf("get key: %v", err)
	}
	if raw != "[]" {
		t.Errorf("stored=%q want '[]'", raw)
	}
}

func TestLoadPromptsToRedis_OnlyNonTxt_WritesEmptyArray(t *testing.T) {
	dir := t.TempDir()
	withPromptsDir(t, dir)
	writeFile(t, dir, "a.md", "x")
	writeFile(t, dir, "b.json", "x")

	mr := miniredis.RunT(t)
	cfg := newTestConfig(t, mr)
	r := newRedisClient(cfg)
	t.Cleanup(func() { _ = r.Close() })

	got, err := loadPromptsToRedis(context.Background(), r, cfg)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if len(got) != 0 {
		t.Errorf("got=%v want empty", got)
	}
	raw, _ := mr.Get(cfg.RedisPromptsKeyName)
	if raw != "[]" {
		t.Errorf("stored=%q want '[]'", raw)
	}
}

func TestLoadPromptsToRedis_MissingDir_ReturnsError(t *testing.T) {
	// Point at a path that doesn't exist.
	withPromptsDir(t, filepath.Join(t.TempDir(), "does-not-exist"))

	mr := miniredis.RunT(t)
	cfg := newTestConfig(t, mr)
	r := newRedisClient(cfg)
	t.Cleanup(func() { _ = r.Close() })

	_, err := loadPromptsToRedis(context.Background(), r, cfg)
	if err == nil {
		t.Fatal("want error for missing prompts dir")
	}
	if !strings.Contains(err.Error(), "failed to list files in directory") {
		t.Errorf("err=%v missing expected text", err)
	}
}

// ---------------------------------------------------------------------------
// waitForSessionInit
// ---------------------------------------------------------------------------

func TestWaitForSessionInit_KeyAlreadyPresent_ReturnsImmediately(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := newTestConfig(t, mr)
	r := newRedisClient(cfg)
	t.Cleanup(func() { _ = r.Close() })

	mr.Set(cfg.SessionIDKey, "sid")

	done := make(chan error, 1)
	go func() { done <- waitForSessionInit(context.Background(), r, cfg) }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("err=%v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("did not return quickly when key was already set")
	}
}

func TestWaitForSessionInit_ContextCanceled(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := newTestConfig(t, mr)
	r := newRedisClient(cfg)
	t.Cleanup(func() { _ = r.Close() })

	// Key is not set. Cancel the context and expect a quick return.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan error, 1)
	go func() { done <- waitForSessionInit(ctx, r, cfg) }()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("want error on canceled context")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("did not return within 2s of ctx cancel")
	}
}

func TestWaitForSessionInit_KeyAppearsLater(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping slow test in -short mode")
	}
	mr := miniredis.RunT(t)
	cfg := newTestConfig(t, mr)
	r := newRedisClient(cfg)
	t.Cleanup(func() { _ = r.Close() })

	done := make(chan error, 1)
	go func() { done <- waitForSessionInit(context.Background(), r, cfg) }()

	// Set the key partway through the loop's 1s wait.
	go func() {
		time.Sleep(200 * time.Millisecond)
		mr.Set(cfg.SessionIDKey, "sid")
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("err=%v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("did not detect key set mid-loop within 3s")
	}
}

func TestWaitForSessionInit_RedisError_Returns(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := newTestConfig(t, mr)
	r := newRedisClient(cfg)
	t.Cleanup(func() { _ = r.Close() })
	mr.Close()

	done := make(chan error, 1)
	go func() { done <- waitForSessionInit(context.Background(), r, cfg) }()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("want error when Redis unreachable")
		}
		if !strings.Contains(err.Error(), "failed to check if key") {
			t.Errorf("err=%v missing expected text", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("did not return within 2s of Redis outage")
	}
}
