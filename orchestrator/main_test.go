package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// newTestServer spins up an in-memory Redis (miniredis) and returns a Server
// pointed at it. Cleanup is registered via t.Cleanup.
func newTestServer(t *testing.T) (*Server, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)

	cfg := &config{
		RedisAddress:          mr.Host(),
		RedisPort:             mustAtoi(t, mr.Port()),
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
		redisClient: redis.NewClient(&redis.Options{
			Addr: fmt.Sprintf("%s:%d", cfg.RedisAddress, cfg.RedisPort),
		}),
		keepaliveKey:         cfg.WebpageKeepaliveKey,
		keepaliveCheckTicker: time.NewTicker(time.Second),
		canceled:             make(chan struct{}),
		cfg:                  cfg,
	}

	t.Cleanup(func() {
		srv.keepaliveCheckTicker.Stop()
		_ = srv.redisClient.Close()
	})
	return srv, mr
}

func mustAtoi(t *testing.T, s string) int {
	t.Helper()
	var n int
	if _, err := fmt.Sscanf(s, "%d", &n); err != nil {
		t.Fatalf("bad port %q: %v", s, err)
	}
	return n
}

// ---------------------------------------------------------------------------
// NewServer
// ---------------------------------------------------------------------------

func TestNewServer_WiresConfig(t *testing.T) {
	cfg := &config{
		RedisAddress:        "127.0.0.1",
		RedisPort:           6379,
		WebpageKeepaliveKey: "some:key",
	}
	srv, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	t.Cleanup(func() {
		srv.keepaliveCheckTicker.Stop()
		_ = srv.redisClient.Close()
	})

	if srv.redisClient == nil {
		t.Fatal("redisClient nil")
	}
	if srv.keepaliveKey != "some:key" {
		t.Errorf("keepaliveKey=%q want some:key", srv.keepaliveKey)
	}
	if srv.keepaliveCheckTicker == nil {
		t.Error("keepaliveCheckTicker nil")
	}
	if srv.canceled == nil {
		t.Error("canceled channel nil")
	}
	if srv.cfg != cfg {
		t.Error("cfg pointer not preserved")
	}
}

// NewServer does not verify Redis connectivity; document that behavior so a
// future refactor that adds validation surfaces here rather than silently
// changing behavior.
func TestNewServer_NoConnectivityCheck(t *testing.T) {
	cfg := &config{RedisAddress: "!!!not-a-host!!!", RedisPort: 1}
	srv, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer returned err for a bad address; expected lazy construction: %v", err)
	}
	srv.keepaliveCheckTicker.Stop()
	_ = srv.redisClient.Close()
}

// ---------------------------------------------------------------------------
// checkKeepalive
// ---------------------------------------------------------------------------

func TestCheckKeepalive_MissingKey_NoOp(t *testing.T) {
	srv, mr := newTestServer(t)

	mr.Set(srv.cfg.WebpageKeepaliveKey, "true")

	srv.checkKeepalive(context.Background(), 5*time.Second)

	got, _ := mr.Get(srv.cfg.WebpageKeepaliveKey)
	if got != "true" {
		t.Errorf("keepalive=%q want unchanged 'true'", got)
	}
}

func TestCheckKeepalive_RecentHeartbeat_NoOp(t *testing.T) {
	srv, mr := newTestServer(t)

	mr.Set(srv.cfg.LastHeartbeatKey, fmt.Sprintf("%d", time.Now().Unix()))
	mr.Set(srv.cfg.WebpageKeepaliveKey, "true")

	srv.checkKeepalive(context.Background(), 5*time.Second)

	got, _ := mr.Get(srv.cfg.WebpageKeepaliveKey)
	if got != "true" {
		t.Errorf("keepalive=%q want unchanged 'true'", got)
	}
}

func TestCheckKeepalive_ExpiredHeartbeat_SetsFalse(t *testing.T) {
	srv, mr := newTestServer(t)

	mr.Set(srv.cfg.LastHeartbeatKey, fmt.Sprintf("%d", time.Now().Add(-time.Hour).Unix()))
	mr.Set(srv.cfg.WebpageKeepaliveKey, "true")

	srv.checkKeepalive(context.Background(), 5*time.Second)

	got, err := mr.Get(srv.cfg.WebpageKeepaliveKey)
	if err != nil {
		t.Fatalf("get keepalive: %v", err)
	}
	// go-redis serialises bool false as "0"
	if got != "0" && got != "false" {
		t.Errorf("keepalive=%q want falsy", got)
	}
}

func TestCheckKeepalive_MalformedHeartbeat_NoOp(t *testing.T) {
	srv, mr := newTestServer(t)

	mr.Set(srv.cfg.LastHeartbeatKey, "not-an-int")
	mr.Set(srv.cfg.WebpageKeepaliveKey, "true")

	srv.checkKeepalive(context.Background(), 5*time.Second)

	got, _ := mr.Get(srv.cfg.WebpageKeepaliveKey)
	if got != "true" {
		t.Errorf("keepalive=%q want unchanged 'true' after parse error", got)
	}
}

func TestCheckKeepalive_CanceledContext_NoOp(t *testing.T) {
	srv, mr := newTestServer(t)

	mr.Set(srv.cfg.LastHeartbeatKey, fmt.Sprintf("%d", time.Now().Add(-time.Hour).Unix()))
	mr.Set(srv.cfg.WebpageKeepaliveKey, "true")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	srv.checkKeepalive(ctx, 5*time.Second)

	got, _ := mr.Get(srv.cfg.WebpageKeepaliveKey)
	if got != "true" {
		t.Errorf("keepalive=%q want unchanged 'true' on canceled ctx", got)
	}
}

func TestCheckKeepalive_RedisDown_NoPanic(t *testing.T) {
	srv, mr := newTestServer(t)
	mr.Close()

	// Must not panic even when Redis is completely unavailable.
	srv.checkKeepalive(context.Background(), 5*time.Second)
}

// ---------------------------------------------------------------------------
// startKeepaliveMonitor
// ---------------------------------------------------------------------------

func TestStartKeepaliveMonitor_ExitsOnCtxCancel(t *testing.T) {
	srv, _ := newTestServer(t)

	srv.keepaliveCheckTicker.Stop()
	srv.keepaliveCheckTicker = time.NewTicker(10 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	srv.startKeepaliveMonitor(ctx, 5*time.Second)

	time.Sleep(50 * time.Millisecond)
	cancel()

	done := make(chan struct{})
	go func() {
		srv.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("keepalive monitor did not exit within 2s of ctx cancel")
	}
}

func TestStartKeepaliveMonitor_TicksInvokeCheck(t *testing.T) {
	srv, mr := newTestServer(t)

	mr.Set(srv.cfg.LastHeartbeatKey, fmt.Sprintf("%d", time.Now().Add(-time.Hour).Unix()))
	mr.Set(srv.cfg.WebpageKeepaliveKey, "true")

	srv.keepaliveCheckTicker.Stop()
	srv.keepaliveCheckTicker = time.NewTicker(20 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	srv.startKeepaliveMonitor(ctx, 5*time.Second)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		got, _ := mr.Get(srv.cfg.WebpageKeepaliveKey)
		if got == "0" || got == "false" {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	got, _ := mr.Get(srv.cfg.WebpageKeepaliveKey)
	t.Fatalf("keepalive never flipped to false; last value=%q", got)
}

// ---------------------------------------------------------------------------
// handleWebpageHeartbeat
// ---------------------------------------------------------------------------

func TestHandleWebpageHeartbeat_WrongMethod(t *testing.T) {
	srv, _ := newTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/webpage-keepalive", nil)
	w := httptest.NewRecorder()
	srv.handleWebpageHeartbeat(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("status=%d want 405", w.Code)
	}
}

func TestHandleWebpageHeartbeat_HappyPath(t *testing.T) {
	srv, mr := newTestServer(t)

	before := time.Now().Unix()
	req := httptest.NewRequest(http.MethodPost, "/webpage-keepalive", nil)
	w := httptest.NewRecorder()
	srv.handleWebpageHeartbeat(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status=%d want 200; body=%s", w.Code, w.Body.String())
	}
	if w.Body.String() != "OK" {
		t.Errorf("body=%q want OK", w.Body.String())
	}

	hbStr, err := mr.Get(srv.cfg.LastHeartbeatKey)
	if err != nil {
		t.Fatalf("get heartbeat: %v", err)
	}
	var hb int64
	fmt.Sscanf(hbStr, "%d", &hb)
	if hb < before {
		t.Errorf("heartbeat=%d recorded before request start=%d", hb, before)
	}

	ka, _ := mr.Get(srv.cfg.WebpageKeepaliveKey)
	if ka != "1" && ka != "true" {
		t.Errorf("keepalive=%q want truthy", ka)
	}
}

func TestHandleWebpageHeartbeat_RedisDown_500(t *testing.T) {
	srv, mr := newTestServer(t)
	mr.Close()

	req := httptest.NewRequest(http.MethodPost, "/webpage-keepalive", nil)
	w := httptest.NewRecorder()
	srv.handleWebpageHeartbeat(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("status=%d want 500", w.Code)
	}
	if !strings.Contains(w.Body.String(), "failed to write heartbeat") {
		t.Errorf("body=%q missing expected error text", w.Body.String())
	}
}

// Force a Redis error via miniredis.SetError. Both SET calls in the handler
// will fail, exercising the error-return branches (the handler bails on the
// first error, so this hits the heartbeat-write branch).
func TestHandleWebpageHeartbeat_SetError_500(t *testing.T) {
	srv, mr := newTestServer(t)

	mr.SetError("boom")

	req := httptest.NewRequest(http.MethodPost, "/webpage-keepalive", nil)
	w := httptest.NewRecorder()
	srv.handleWebpageHeartbeat(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("status=%d want 500", w.Code)
	}
}

// ---------------------------------------------------------------------------
// handleWebpageStatus
// ---------------------------------------------------------------------------

func TestHandleWebpageStatus_WrongMethod(t *testing.T) {
	srv, _ := newTestServer(t)

	req := httptest.NewRequest(http.MethodPost, "/webpage_status.json", nil)
	w := httptest.NewRecorder()
	srv.handleWebpageStatus(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("status=%d want 405", w.Code)
	}
}

func TestHandleWebpageStatus_MissingKeys_Unknown(t *testing.T) {
	srv, _ := newTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/webpage_status.json", nil)
	w := httptest.NewRecorder()
	srv.handleWebpageStatus(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status=%d want 200", w.Code)
	}
	var resp StatusResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.OpenAITextGeneratorStatus.Status != "unknown" {
		t.Errorf("openai status=%q want unknown", resp.OpenAITextGeneratorStatus.Status)
	}
	if resp.QwenTTSSpeakerStatus.Status != "unknown" {
		t.Errorf("qwen status=%q want unknown", resp.QwenTTSSpeakerStatus.Status)
	}
}

func TestHandleWebpageStatus_ValidJSON(t *testing.T) {
	srv, mr := newTestServer(t)

	mr.Set(srv.cfg.RedisStatusOutputName, `{"status":"ok","as_of":1234}`)
	mr.Set(srv.cfg.QwenTTSSpeakerKey, `{"status":"idle","as_of":5678}`)

	req := httptest.NewRequest(http.MethodGet, "/webpage_status.json", nil)
	w := httptest.NewRecorder()
	srv.handleWebpageStatus(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status=%d want 200", w.Code)
	}
	var resp StatusResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.OpenAITextGeneratorStatus.Status != "ok" || resp.OpenAITextGeneratorStatus.AsOf != 1234 {
		t.Errorf("openai=%+v", resp.OpenAITextGeneratorStatus)
	}
	if resp.QwenTTSSpeakerStatus.Status != "idle" || resp.QwenTTSSpeakerStatus.AsOf != 5678 {
		t.Errorf("qwen=%+v", resp.QwenTTSSpeakerStatus)
	}
}

func TestHandleWebpageStatus_MalformedOpenAI_Error(t *testing.T) {
	srv, mr := newTestServer(t)

	mr.Set(srv.cfg.RedisStatusOutputName, `not json`)
	mr.Set(srv.cfg.QwenTTSSpeakerKey, `{"status":"idle","as_of":5678}`)

	req := httptest.NewRequest(http.MethodGet, "/webpage_status.json", nil)
	w := httptest.NewRecorder()
	srv.handleWebpageStatus(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status=%d want 200", w.Code)
	}
	var resp StatusResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.OpenAITextGeneratorStatus.Status != "error" {
		t.Errorf("openai status=%q want error", resp.OpenAITextGeneratorStatus.Status)
	}
	if resp.QwenTTSSpeakerStatus.Status != "idle" {
		t.Errorf("qwen status=%q want idle", resp.QwenTTSSpeakerStatus.Status)
	}
}

func TestHandleWebpageStatus_MalformedQwen_Error(t *testing.T) {
	srv, mr := newTestServer(t)

	mr.Set(srv.cfg.RedisStatusOutputName, `{"status":"ok","as_of":1}`)
	mr.Set(srv.cfg.QwenTTSSpeakerKey, `not json`)

	req := httptest.NewRequest(http.MethodGet, "/webpage_status.json", nil)
	w := httptest.NewRecorder()
	srv.handleWebpageStatus(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status=%d want 200", w.Code)
	}
	var resp StatusResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.OpenAITextGeneratorStatus.Status != "ok" {
		t.Errorf("openai status=%q want ok", resp.OpenAITextGeneratorStatus.Status)
	}
	if resp.QwenTTSSpeakerStatus.Status != "error" {
		t.Errorf("qwen status=%q want error", resp.QwenTTSSpeakerStatus.Status)
	}
}

func TestHandleWebpageStatus_RedisDown_ErrorStatuses(t *testing.T) {
	srv, mr := newTestServer(t)
	mr.Close()

	req := httptest.NewRequest(http.MethodGet, "/webpage_status.json", nil)
	w := httptest.NewRecorder()
	srv.handleWebpageStatus(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status=%d want 200", w.Code)
	}
	var resp StatusResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.OpenAITextGeneratorStatus.Status != "error" {
		t.Errorf("openai status=%q want error", resp.OpenAITextGeneratorStatus.Status)
	}
	if resp.QwenTTSSpeakerStatus.Status != "error" {
		t.Errorf("qwen status=%q want error", resp.QwenTTSSpeakerStatus.Status)
	}
}

// ---------------------------------------------------------------------------
// handleUpdateSession
// ---------------------------------------------------------------------------

func TestHandleUpdateSession_WrongMethod(t *testing.T) {
	srv, _ := newTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/update_session?voice=v&prompt=p", nil)
	w := httptest.NewRecorder()
	srv.handleUpdateSession(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("status=%d want 405", w.Code)
	}
}

func TestHandleUpdateSession_MissingVoice(t *testing.T) {
	srv, _ := newTestServer(t)

	req := httptest.NewRequest(http.MethodPost, "/update_session?prompt=p", nil)
	w := httptest.NewRecorder()
	srv.handleUpdateSession(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status=%d want 400", w.Code)
	}
}

func TestHandleUpdateSession_MissingPrompt(t *testing.T) {
	srv, _ := newTestServer(t)

	req := httptest.NewRequest(http.MethodPost, "/update_session?voice=v", nil)
	w := httptest.NewRecorder()
	srv.handleUpdateSession(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status=%d want 400", w.Code)
	}
}

func TestHandleUpdateSession_BothMissing(t *testing.T) {
	srv, _ := newTestServer(t)

	req := httptest.NewRequest(http.MethodPost, "/update_session", nil)
	w := httptest.NewRecorder()
	srv.handleUpdateSession(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status=%d want 400", w.Code)
	}
}

func TestHandleUpdateSession_EmptyValues(t *testing.T) {
	srv, _ := newTestServer(t)

	req := httptest.NewRequest(http.MethodPost, "/update_session?voice=&prompt=", nil)
	w := httptest.NewRecorder()
	srv.handleUpdateSession(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status=%d want 400", w.Code)
	}
}

func TestHandleUpdateSession_HappyPath(t *testing.T) {
	srv, mr := newTestServer(t)

	req := httptest.NewRequest(http.MethodPost, "/update_session?voice=alice&prompt=greet", nil)
	w := httptest.NewRecorder()
	srv.handleUpdateSession(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status=%d want 200; body=%s", w.Code, w.Body.String())
	}

	sid, err := mr.Get(srv.cfg.SessionIDKey)
	if err != nil {
		t.Fatalf("session id: %v", err)
	}
	if sid != "greet" {
		t.Errorf("session id=%q want greet", sid)
	}

	info, err := mr.Get(srv.cfg.SessionInfoKey)
	if err != nil {
		t.Fatalf("session info: %v", err)
	}
	var s SessionInfo
	if err := json.Unmarshal([]byte(info), &s); err != nil {
		t.Fatalf("unmarshal session info: %v", err)
	}
	if s.VoiceID != "alice" || s.PromptID != "greet" {
		t.Errorf("session=%+v", s)
	}
	if s.AsOf == 0 {
		t.Error("session.AsOf zero")
	}
}

func TestHandleUpdateSession_RedisDown_500(t *testing.T) {
	srv, mr := newTestServer(t)
	mr.Close()

	req := httptest.NewRequest(http.MethodPost, "/update_session?voice=v&prompt=p", nil)
	w := httptest.NewRecorder()
	srv.handleUpdateSession(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("status=%d want 500", w.Code)
	}
}

// ---------------------------------------------------------------------------
// handleSpeechOptions
// ---------------------------------------------------------------------------

func TestHandleSpeechOptions_WrongMethod(t *testing.T) {
	srv, _ := newTestServer(t)

	req := httptest.NewRequest(http.MethodPost, "/speech_options", nil)
	w := httptest.NewRecorder()
	srv.handleSpeechOptions(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("status=%d want 405", w.Code)
	}
}

func TestHandleSpeechOptions_HappyPath(t *testing.T) {
	srv, mr := newTestServer(t)

	mr.Set(srv.cfg.TTSVoiceOptionsKey, `["alice","bob"]`)
	mr.Set(srv.cfg.PromptOptionsKey, `["greet","goodbye"]`)

	req := httptest.NewRequest(http.MethodGet, "/speech_options", nil)
	w := httptest.NewRecorder()
	srv.handleSpeechOptions(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status=%d want 200; body=%s", w.Code, w.Body.String())
	}
	var got SpeechOptions
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(got.Voices) != 2 || got.Voices[0] != "alice" || got.Voices[1] != "bob" {
		t.Errorf("voices=%v", got.Voices)
	}
	if len(got.Prompts) != 2 || got.Prompts[0] != "greet" || got.Prompts[1] != "goodbye" {
		t.Errorf("prompts=%v", got.Prompts)
	}
}

func TestHandleSpeechOptions_MissingVoicesKey(t *testing.T) {
	srv, mr := newTestServer(t)

	mr.Set(srv.cfg.PromptOptionsKey, `["greet"]`)

	req := httptest.NewRequest(http.MethodGet, "/speech_options", nil)
	w := httptest.NewRecorder()
	srv.handleSpeechOptions(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status=%d want 200; body=%s", w.Code, w.Body.String())
	}
	var got SpeechOptions
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(got.Prompts) != 1 || got.Prompts[0] != "greet" {
		t.Errorf("prompts=%v", got.Prompts)
	}
}

func TestHandleSpeechOptions_MissingPromptsKey(t *testing.T) {
	srv, mr := newTestServer(t)

	// Only voice options set. Current implementation clears `voices` when
	// the prompt fetch fails (see handleSpeechOptions), so only assert 200.
	mr.Set(srv.cfg.TTSVoiceOptionsKey, `["alice"]`)

	req := httptest.NewRequest(http.MethodGet, "/speech_options", nil)
	w := httptest.NewRecorder()
	srv.handleSpeechOptions(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status=%d want 200; body=%s", w.Code, w.Body.String())
	}
	var got SpeechOptions
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
}

func TestHandleSpeechOptions_MalformedVoiceJSON(t *testing.T) {
	srv, mr := newTestServer(t)

	mr.Set(srv.cfg.TTSVoiceOptionsKey, `not json`)
	mr.Set(srv.cfg.PromptOptionsKey, `["p"]`)

	req := httptest.NewRequest(http.MethodGet, "/speech_options", nil)
	w := httptest.NewRecorder()
	srv.handleSpeechOptions(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status=%d want 200; body=%s", w.Code, w.Body.String())
	}
	var got SpeechOptions
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(got.Voices) != 0 {
		t.Errorf("voices=%v want empty on malformed json", got.Voices)
	}
	if len(got.Prompts) != 1 {
		t.Errorf("prompts=%v want length 1", got.Prompts)
	}
}

func TestHandleSpeechOptions_MalformedPromptJSON(t *testing.T) {
	srv, mr := newTestServer(t)

	mr.Set(srv.cfg.TTSVoiceOptionsKey, `["v"]`)
	mr.Set(srv.cfg.PromptOptionsKey, `not json`)

	req := httptest.NewRequest(http.MethodGet, "/speech_options", nil)
	w := httptest.NewRecorder()
	srv.handleSpeechOptions(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status=%d want 200; body=%s", w.Code, w.Body.String())
	}
	var got SpeechOptions
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(got.Prompts) != 0 {
		t.Errorf("prompts=%v want empty", got.Prompts)
	}
}

func TestHandleSpeechOptions_BothFail_500(t *testing.T) {
	srv, mr := newTestServer(t)
	mr.Close()

	req := httptest.NewRequest(http.MethodGet, "/speech_options", nil)
	w := httptest.NewRecorder()
	srv.handleSpeechOptions(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("status=%d want 500", w.Code)
	}
	if !strings.Contains(w.Body.String(), "Failed to fetch any options data") {
		t.Errorf("body=%q missing expected error text", w.Body.String())
	}
}

// ---------------------------------------------------------------------------
// handleHealth
// ---------------------------------------------------------------------------

func TestHandleHealth_WrongMethod(t *testing.T) {
	srv, _ := newTestServer(t)

	req := httptest.NewRequest(http.MethodPost, "/health", nil)
	w := httptest.NewRecorder()
	srv.handleHealth(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("status=%d want 405", w.Code)
	}
}

func TestHandleHealth_OK(t *testing.T) {
	srv, _ := newTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	srv.handleHealth(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status=%d want 200", w.Code)
	}
	var got map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got["status"] != "healthy" {
		t.Errorf("status=%q want healthy", got["status"])
	}
}
