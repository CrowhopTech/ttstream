package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/plexusone/elevenlabs-go/tts"
	"github.com/redis/go-redis/v9"
)

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

func mustAtoi(t *testing.T, s string) int {
	t.Helper()
	var n int
	if _, err := fmt.Sscanf(s, "%d", &n); err != nil {
		t.Fatalf("bad int %q: %v", s, err)
	}
	return n
}

// newTestConfig returns a config populated with the same defaults as the real
// envDefaults, pointed at the given miniredis instance.
func newTestConfig(t *testing.T, mr *miniredis.Miniredis) *config {
	t.Helper()
	return &config{
		APIKey:                "test-key",
		Model:                 "eleven_turbo_v2_5",
		RedisAddress:          mr.Host(),
		RedisPort:             mustAtoi(t, mr.Port()),
		RedisStatusOutputName: "statuses:elevenlabs_tts_speaker",
		RedisTriggerKeyName:   "session:webpage_connected",
		SessionIDKey:          "session:id",
		SessionInfoKey:        "session:info",
		RedisVoicesKeyName:    "options:tts_voices",
		InputQueueBase:        "queues:generated_text",
		OutputQueueBase:       "queues:generated_audio_bytes",
		VoicesFilePath:        "voices.json",
	}
}

func newRedisClient(cfg *config) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("%s:%d", cfg.RedisAddress, cfg.RedisPort),
	})
}

// writeVoicesFile writes a JSON voices file to a temp dir and returns its path.
func writeVoicesFile(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "voices.json")
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("write voices file: %v", err)
	}
	return path
}

// fakeTTS is a swappable stand-in for tts.Service.
type fakeTTS struct {
	mu       sync.Mutex
	calls    []tts.Request
	response []byte
	err      error
}

func (f *fakeTTS) Generate(ctx context.Context, req *tts.Request) (*tts.Response, error) {
	f.mu.Lock()
	f.calls = append(f.calls, *req)
	f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	return &tts.Response{Audio: bytes.NewReader(f.response)}, nil
}

// int16BytesLE packs int16 samples in little-endian byte order for use as the
// fake TTS PCM stream.
func int16BytesLE(samples ...int16) []byte {
	out := make([]byte, len(samples)*2)
	for i, s := range samples {
		binary.LittleEndian.PutUint16(out[i*2:i*2+2], uint16(s))
	}
	return out
}

// decodeFloat32LE unpacks a byte slice into float32s.
func decodeFloat32LE(b []byte) []float32 {
	out := make([]float32, len(b)/4)
	for i := range out {
		out[i] = math.Float32frombits(binary.LittleEndian.Uint32(b[i*4 : i*4+4]))
	}
	return out
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
	var payload ElevenLabsTTSSpeakerStatus
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		t.Fatalf("unmarshal: %v (raw=%q)", err, raw)
	}
	if payload.Status != "hello" {
		t.Errorf("status=%q want hello", payload.Status)
	}
	if payload.AsOf == 0 {
		t.Errorf("as_of not populated")
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
// loadVoices
// ---------------------------------------------------------------------------

func TestLoadVoices_HappyPath(t *testing.T) {
	path := writeVoicesFile(t, `{"alice":{"elevenlabs_voice_id":"vid-1"},"bob":{"elevenlabs_voice_id":"vid-2"}}`)
	got, err := loadVoices(path)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if got["alice"].ElevenLabsVoiceID != "vid-1" {
		t.Errorf("alice=%+v", got["alice"])
	}
	if got["bob"].ElevenLabsVoiceID != "vid-2" {
		t.Errorf("bob=%+v", got["bob"])
	}
}

func TestLoadVoices_MissingFile(t *testing.T) {
	_, err := loadVoices(filepath.Join(t.TempDir(), "nope.json"))
	if err == nil {
		t.Fatal("want error for missing file")
	}
	if !strings.Contains(err.Error(), "failed to read voices file") {
		t.Errorf("err=%v missing expected text", err)
	}
}

func TestLoadVoices_MalformedJSON(t *testing.T) {
	path := writeVoicesFile(t, `not-json`)
	_, err := loadVoices(path)
	if err == nil {
		t.Fatal("want error for malformed JSON")
	}
	if !strings.Contains(err.Error(), "failed to parse voices file") {
		t.Errorf("err=%v missing expected text", err)
	}
}

func TestLoadVoices_EmptyObject(t *testing.T) {
	path := writeVoicesFile(t, `{}`)
	_, err := loadVoices(path)
	if err == nil {
		t.Fatal("want error for empty voices object")
	}
	if !strings.Contains(err.Error(), "contains no entries") {
		t.Errorf("err=%v missing expected text", err)
	}
}

// ---------------------------------------------------------------------------
// publishVoices
// ---------------------------------------------------------------------------

func TestPublishVoices_WritesJSONBlob(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := newTestConfig(t, mr)
	r := newRedisClient(cfg)
	t.Cleanup(func() { _ = r.Close() })

	in := map[string]Voice{"a": {ElevenLabsVoiceID: "vid-a"}}
	if err := publishVoices(context.Background(), r, cfg, in); err != nil {
		t.Fatalf("err=%v", err)
	}

	raw, err := mr.Get(cfg.RedisVoicesKeyName)
	if err != nil {
		t.Fatalf("get voices key: %v", err)
	}
	var got map[string]Voice
	if err := json.Unmarshal([]byte(raw), &got); err != nil {
		t.Fatalf("unmarshal: %v (raw=%q)", err, raw)
	}
	if got["a"].ElevenLabsVoiceID != "vid-a" {
		t.Errorf("got=%+v", got)
	}
}

func TestPublishVoices_RedisDown_ReturnsErr(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := newTestConfig(t, mr)
	r := newRedisClient(cfg)
	t.Cleanup(func() { _ = r.Close() })
	mr.Close()

	err := publishVoices(context.Background(), r, cfg, map[string]Voice{"a": {ElevenLabsVoiceID: "x"}})
	if err == nil {
		t.Fatal("want error when Redis is unreachable")
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

// ---------------------------------------------------------------------------
// resolveVoiceID
// ---------------------------------------------------------------------------

func TestResolveVoiceID_UsesSessionInfoVoice(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := newTestConfig(t, mr)
	r := newRedisClient(cfg)
	t.Cleanup(func() { _ = r.Close() })

	mr.Set(cfg.SessionInfoKey, `{"as_of":1,"voice_id":"alice","prompt_id":"p"}`)
	voices := map[string]Voice{"alice": {ElevenLabsVoiceID: "vid-alice"}, "bob": {ElevenLabsVoiceID: "vid-bob"}}

	got, err := resolveVoiceID(context.Background(), r, cfg, voices)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if got != "vid-alice" {
		t.Errorf("got=%q want vid-alice", got)
	}
}

func TestResolveVoiceID_MissingSessionInfoFallsBack(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := newTestConfig(t, mr)
	r := newRedisClient(cfg)
	t.Cleanup(func() { _ = r.Close() })

	voices := map[string]Voice{"only": {ElevenLabsVoiceID: "vid-only"}}
	got, err := resolveVoiceID(context.Background(), r, cfg, voices)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if got != "vid-only" {
		t.Errorf("got=%q want vid-only", got)
	}
}

func TestResolveVoiceID_EmptyVoiceIDFallsBack(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := newTestConfig(t, mr)
	r := newRedisClient(cfg)
	t.Cleanup(func() { _ = r.Close() })

	mr.Set(cfg.SessionInfoKey, `{"as_of":1,"voice_id":"","prompt_id":"p"}`)
	voices := map[string]Voice{"only": {ElevenLabsVoiceID: "vid-only"}}

	got, err := resolveVoiceID(context.Background(), r, cfg, voices)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if got != "vid-only" {
		t.Errorf("got=%q want vid-only", got)
	}
}

func TestResolveVoiceID_MalformedInfoFallsBack(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := newTestConfig(t, mr)
	r := newRedisClient(cfg)
	t.Cleanup(func() { _ = r.Close() })

	mr.Set(cfg.SessionInfoKey, "not-json")
	voices := map[string]Voice{"only": {ElevenLabsVoiceID: "vid-only"}}

	got, err := resolveVoiceID(context.Background(), r, cfg, voices)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if got != "vid-only" {
		t.Errorf("got=%q want vid-only", got)
	}
}

func TestResolveVoiceID_UnknownVoiceFallsBack(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := newTestConfig(t, mr)
	r := newRedisClient(cfg)
	t.Cleanup(func() { _ = r.Close() })

	mr.Set(cfg.SessionInfoKey, `{"as_of":1,"voice_id":"ghost","prompt_id":"p"}`)
	voices := map[string]Voice{"only": {ElevenLabsVoiceID: "vid-only"}}

	got, err := resolveVoiceID(context.Background(), r, cfg, voices)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if got != "vid-only" {
		t.Errorf("got=%q want vid-only", got)
	}
}

func TestResolveVoiceID_NoVoicesReturnsError(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := newTestConfig(t, mr)
	r := newRedisClient(cfg)
	t.Cleanup(func() { _ = r.Close() })

	// no session info, no voices
	_, err := resolveVoiceID(context.Background(), r, cfg, map[string]Voice{})
	if err == nil {
		t.Fatal("want error when voices map is empty")
	}
	if !strings.Contains(err.Error(), "no voices available") {
		t.Errorf("err=%v missing expected text", err)
	}
}

func TestResolveVoiceID_RedisDown_ReturnsErr(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := newTestConfig(t, mr)
	r := newRedisClient(cfg)
	t.Cleanup(func() { _ = r.Close() })
	mr.Close()

	_, err := resolveVoiceID(context.Background(), r, cfg, map[string]Voice{"only": {ElevenLabsVoiceID: "x"}})
	if err == nil {
		t.Fatal("want error when Redis unreachable")
	}
	if !strings.Contains(err.Error(), "failed to read session info") {
		t.Errorf("err=%v missing expected text", err)
	}
}

// ---------------------------------------------------------------------------
// inputQueueName / outputQueueName
// ---------------------------------------------------------------------------

func TestQueueNames_FormatsWithColon(t *testing.T) {
	cfg := &config{InputQueueBase: "queues:generated_text", OutputQueueBase: "queues:generated_audio_bytes"}
	if got := inputQueueName(cfg, "sid"); got != "queues:generated_text:sid" {
		t.Errorf("input=%q", got)
	}
	if got := outputQueueName(cfg, "sid"); got != "queues:generated_audio_bytes:sid" {
		t.Errorf("output=%q", got)
	}
}

func TestQueueNames_EmptySession(t *testing.T) {
	cfg := &config{InputQueueBase: "a", OutputQueueBase: "b"}
	if got := inputQueueName(cfg, ""); got != "a:" {
		t.Errorf("input=%q", got)
	}
	if got := outputQueueName(cfg, ""); got != "b:" {
		t.Errorf("output=%q", got)
	}
}

// ---------------------------------------------------------------------------
// generateSpeech
// ---------------------------------------------------------------------------

func TestGenerateSpeech_ConvertsInt16ToFloat32(t *testing.T) {
	pcm := int16BytesLE(0, 32767, -32768, 16384, -16384)
	svc := &fakeTTS{response: pcm}

	got, err := generateSpeech(context.Background(), svc, "modelX", "voiceY", "hello")
	if err != nil {
		t.Fatalf("err=%v", err)
	}

	if len(got) != len(pcm)*2 {
		t.Fatalf("output bytes=%d want %d", len(got), len(pcm)*2)
	}
	floats := decodeFloat32LE(got)
	if len(floats) != 5 {
		t.Fatalf("floats=%d want 5", len(floats))
	}

	// 0 → 0, 32767 → ~1 (32767/32768), -32768 → -1, 16384 → 0.5, -16384 → -0.5
	wants := []float32{0, float32(32767.0 / 32768.0), -1, 0.5, -0.5}
	for i, w := range wants {
		if floats[i] != w {
			t.Errorf("floats[%d]=%v want %v", i, floats[i], w)
		}
	}

	if len(svc.calls) != 1 {
		t.Fatalf("calls=%d want 1", len(svc.calls))
	}
	call := svc.calls[0]
	if call.VoiceID != "voiceY" || call.Text != "hello" || call.ModelID != "modelX" || call.OutputFormat != "pcm_24000" {
		t.Errorf("bad call args: %+v", call)
	}
}

func TestGenerateSpeech_EmptyPCM(t *testing.T) {
	svc := &fakeTTS{response: []byte{}}
	got, err := generateSpeech(context.Background(), svc, "m", "v", "t")
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if len(got) != 0 {
		t.Errorf("expected empty output, got %d bytes", len(got))
	}
}

func TestGenerateSpeech_OddByteLength_ReturnsErr(t *testing.T) {
	svc := &fakeTTS{response: []byte{0x01, 0x02, 0x03}}
	_, err := generateSpeech(context.Background(), svc, "m", "v", "t")
	if err == nil {
		t.Fatal("want error for odd-length pcm")
	}
	if !strings.Contains(err.Error(), "not aligned") {
		t.Errorf("err=%v missing expected text", err)
	}
}

func TestGenerateSpeech_ServiceError_Wrapped(t *testing.T) {
	svc := &fakeTTS{err: errors.New("boom")}
	_, err := generateSpeech(context.Background(), svc, "m", "v", "t")
	if err == nil {
		t.Fatal("want error when service errors")
	}
	if !strings.Contains(err.Error(), "elevenlabs generate failed") {
		t.Errorf("err=%v missing wrap", err)
	}
	if !strings.Contains(err.Error(), "boom") {
		t.Errorf("err=%v missing underlying err", err)
	}
}

// erroringReader always returns a read error to simulate an upstream stream
// failure.
type erroringReader struct{}

func (erroringReader) Read(_ []byte) (int, error) { return 0, errors.New("stream boom") }

// fakeTTSReader lets us swap the audio reader directly to test read failures.
type fakeTTSReader struct{ audio io.Reader }

func (f *fakeTTSReader) Generate(_ context.Context, _ *tts.Request) (*tts.Response, error) {
	return &tts.Response{Audio: f.audio}, nil
}

func TestGenerateSpeech_ReadFailure_Wrapped(t *testing.T) {
	_, err := generateSpeech(context.Background(), &fakeTTSReader{audio: erroringReader{}}, "m", "v", "t")
	if err == nil {
		t.Fatal("want error when reading audio fails")
	}
	if !strings.Contains(err.Error(), "failed to read pcm stream") {
		t.Errorf("err=%v missing wrap", err)
	}
}

// ---------------------------------------------------------------------------
// popNextText
// ---------------------------------------------------------------------------

func TestPopNextText_ReturnsWaitingItem(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := newTestConfig(t, mr)
	r := newRedisClient(cfg)
	t.Cleanup(func() { _ = r.Close() })

	queue := "queues:input:1"
	// LPush then RPop yields FIFO ordering.
	if _, err := r.LPush(context.Background(), queue, "hello").Result(); err != nil {
		t.Fatalf("lpush: %v", err)
	}

	got, err := popNextText(context.Background(), r, queue)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if got != "hello" {
		t.Errorf("got=%q want hello", got)
	}
}

func TestPopNextText_WaitsForItem(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping slow test in -short mode")
	}
	mr := miniredis.RunT(t)
	cfg := newTestConfig(t, mr)
	r := newRedisClient(cfg)
	t.Cleanup(func() { _ = r.Close() })

	queue := "queues:input:1"
	done := make(chan string, 1)
	go func() {
		v, _ := popNextText(context.Background(), r, queue)
		done <- v
	}()

	go func() {
		time.Sleep(150 * time.Millisecond)
		_, _ = r.LPush(context.Background(), queue, "arrived").Result()
	}()

	select {
	case got := <-done:
		if got != "arrived" {
			t.Errorf("got=%q want arrived", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("popNextText did not return")
	}
}

func TestPopNextText_ContextCanceled(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := newTestConfig(t, mr)
	r := newRedisClient(cfg)
	t.Cleanup(func() { _ = r.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := popNextText(ctx, r, "queues:input:1")
	if err == nil {
		t.Fatal("want error on canceled context")
	}
}

// ---------------------------------------------------------------------------
// waitForTrigger
// ---------------------------------------------------------------------------

func TestWaitForTrigger_ReturnsWhenSetTrue(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping slow test in -short mode")
	}
	mr := miniredis.RunT(t)
	cfg := newTestConfig(t, mr)
	r := newRedisClient(cfg)
	t.Cleanup(func() { _ = r.Close() })

	done := make(chan error, 1)
	go func() { done <- waitForTrigger(context.Background(), r, cfg) }()

	go func() {
		time.Sleep(200 * time.Millisecond)
		mr.Set(cfg.RedisTriggerKeyName, "true")
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("err=%v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("waitForTrigger did not return within 3s")
	}
}

func TestWaitForTrigger_TreatsFalseAsUnset(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping slow test in -short mode")
	}
	mr := miniredis.RunT(t)
	cfg := newTestConfig(t, mr)
	r := newRedisClient(cfg)
	t.Cleanup(func() { _ = r.Close() })

	mr.Set(cfg.RedisTriggerKeyName, "false")

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	err := waitForTrigger(ctx, r, cfg)
	if err == nil {
		t.Fatal("want context deadline exceeded when trigger is 'false'")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err=%v want DeadlineExceeded", err)
	}
}

func TestWaitForTrigger_ContextCanceled(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := newTestConfig(t, mr)
	r := newRedisClient(cfg)
	t.Cleanup(func() { _ = r.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := waitForTrigger(ctx, r, cfg)
	if err == nil {
		t.Fatal("want error on canceled context")
	}
}

func TestWaitForTrigger_RedisError_ReturnsErr(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := newTestConfig(t, mr)
	r := newRedisClient(cfg)
	t.Cleanup(func() { _ = r.Close() })
	mr.Close()

	err := waitForTrigger(context.Background(), r, cfg)
	if err == nil {
		t.Fatal("want error when Redis unreachable")
	}
	if !strings.Contains(err.Error(), "failed to read trigger key") {
		t.Errorf("err=%v missing expected text", err)
	}
}

// ---------------------------------------------------------------------------
// run: end-to-end loop with fake TTS + miniredis
// ---------------------------------------------------------------------------

// runTestSetup wires up miniredis, a redis client, a fake TTS, and a
// voices.json file for the run-loop tests.
func runTestSetup(t *testing.T) (*miniredis.Miniredis, *redis.Client, *fakeTTS, *config) {
	t.Helper()
	mr := miniredis.RunT(t)
	cfg := newTestConfig(t, mr)
	cfg.VoicesFilePath = writeVoicesFile(t, `{"only":{"elevenlabs_voice_id":"vid-only"}}`)

	r := newRedisClient(cfg)
	t.Cleanup(func() { _ = r.Close() })
	return mr, r, &fakeTTS{response: int16BytesLE(0, 100, -100)}, cfg
}

func TestRun_MissingVoicesFile_ReturnsErr(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := newTestConfig(t, mr)
	cfg.VoicesFilePath = filepath.Join(t.TempDir(), "nope.json")
	r := newRedisClient(cfg)
	t.Cleanup(func() { _ = r.Close() })

	err := run(context.Background(), cfg, r, &fakeTTS{})
	if err == nil {
		t.Fatal("want error when voices file missing")
	}
}

func TestRun_HappyPath_GeneratesAndPushes(t *testing.T) {
	mr, r, svc, cfg := runTestSetup(t)

	// Prep session + trigger so the loop reaches generation quickly.
	mr.Set(cfg.SessionIDKey, "sess-1")
	mr.Set(cfg.SessionInfoKey, `{"as_of":1,"voice_id":"only","prompt_id":"p"}`)
	mr.Set(cfg.RedisTriggerKeyName, "true")

	// Queue up two chunks. Ordering doesn't matter here — we only assert both
	// are consumed and that TTS was called with each of them.
	_, _ = r.LPush(context.Background(), inputQueueName(cfg, "sess-1"), "second", "first").Result()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx, cfg, r, svc) }()

	// Poll until the output queue has both audio blobs, then cancel.
	deadline := time.After(3 * time.Second)
	outputQ := outputQueueName(cfg, "sess-1")
	for {
		n, err := r.LLen(context.Background(), outputQ).Result()
		if err == nil && n >= 2 {
			break
		}
		select {
		case <-deadline:
			cancel()
			t.Fatalf("timeout waiting for output; last llen=%d err=%v", n, err)
		case <-time.After(50 * time.Millisecond):
		}
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("run did not exit after ctx cancel")
	}

	// Warmup + two chunks = at least 3 TTS calls (may include a re-check).
	svc.mu.Lock()
	calls := append([]tts.Request(nil), svc.calls...)
	svc.mu.Unlock()
	if len(calls) < 3 {
		t.Fatalf("calls=%d want >=3", len(calls))
	}
	if calls[0].Text != "Warmup" {
		t.Errorf("first call not warmup: %+v", calls[0])
	}
	// Every call should use pcm_24000, the configured model, and vid-only.
	for i, c := range calls {
		if c.OutputFormat != pcmOutputFmt {
			t.Errorf("call %d output_format=%q want %s", i, c.OutputFormat, pcmOutputFmt)
		}
		if c.VoiceID != "vid-only" {
			t.Errorf("call %d voice=%q want vid-only", i, c.VoiceID)
		}
		if c.ModelID != cfg.Model {
			t.Errorf("call %d model=%q want %s", i, c.ModelID, cfg.Model)
		}
	}

	// Voices should have been published, and status set to "ready" at some point.
	if _, err := mr.Get(cfg.RedisVoicesKeyName); err != nil {
		t.Errorf("voices key not written: %v", err)
	}
	if _, err := mr.Get(cfg.RedisStatusOutputName); err != nil {
		t.Errorf("status key not written: %v", err)
	}
}

func TestRun_TriggerFalse_ClearsOutputQueue(t *testing.T) {
	mr, r, svc, cfg := runTestSetup(t)

	mr.Set(cfg.SessionIDKey, "sess-1")
	mr.Set(cfg.SessionInfoKey, `{"as_of":1,"voice_id":"only","prompt_id":"p"}`)
	// Trigger unset → first iteration clears the output queue then waits.
	outputQ := outputQueueName(cfg, "sess-1")
	_, _ = r.LPush(context.Background(), outputQ, "leftover-1", "leftover-2").Result()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx, cfg, r, svc) }()

	// Wait for the queue to be emptied by the loop's Del call.
	deadline := time.After(3 * time.Second)
	for {
		n, err := r.LLen(context.Background(), outputQ).Result()
		if err == nil && n == 0 {
			break
		}
		select {
		case <-deadline:
			cancel()
			t.Fatalf("timeout waiting for output queue clear; last llen=%d err=%v", n, err)
		case <-time.After(50 * time.Millisecond):
		}
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("run did not exit after ctx cancel")
	}
}

// ---------------------------------------------------------------------------
// run: additional error branches
// ---------------------------------------------------------------------------

// waitForRunExit drains the done channel with a timeout so a failing test
// doesn't hang the whole suite.
func waitForRunExit(t *testing.T, done <-chan error, dur time.Duration) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(dur):
		t.Fatal("run did not exit within timeout")
		return nil
	}
}

func TestRun_PublishVoicesRedisDown_ReturnsErr(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := newTestConfig(t, mr)
	cfg.VoicesFilePath = writeVoicesFile(t, `{"only":{"elevenlabs_voice_id":"vid-only"}}`)
	r := newRedisClient(cfg)
	t.Cleanup(func() { _ = r.Close() })
	mr.Close()

	err := run(context.Background(), cfg, r, &fakeTTS{})
	if err == nil {
		t.Fatal("want error when publishVoices fails")
	}
	if !strings.Contains(err.Error(), "failed to write voices to Redis") {
		t.Errorf("err=%v missing expected wrap", err)
	}
}

func TestRun_WaitForSessionInitCanceled_ReturnsErr(t *testing.T) {
	mr, r, svc, cfg := runTestSetup(t)
	_ = mr

	// No session key is set, and context is already canceled — waitForSessionInit
	// should surface ctx.Err() promptly.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := run(ctx, cfg, r, svc)
	if err == nil {
		t.Fatal("want error when ctx canceled before session init")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err=%v want context.Canceled", err)
	}
}

// getFailingRedis wraps a real redis client so a single command name can be
// forced to fail on the next call. It's used to hit the "session ID read
// fails right after init" and "LPush fails" branches deterministically —
// neither is easily triggered by killing miniredis mid-loop.
type failingRedis struct {
	inner redisClient
	// mu guards failGet/failLPush.
	mu         sync.Mutex
	failGetKey string // if non-empty, next Get on this key errors and clears itself
	failLPush  bool   // if true, next LPush call errors and clears itself
}

func (f *failingRedis) Get(ctx context.Context, key string) *redis.StringCmd {
	f.mu.Lock()
	shouldFail := f.failGetKey == key
	if shouldFail {
		f.failGetKey = ""
	}
	f.mu.Unlock()
	if shouldFail {
		cmd := redis.NewStringCmd(ctx, "get", key)
		cmd.SetErr(errors.New("injected get failure"))
		return cmd
	}
	return f.inner.Get(ctx, key)
}

func (f *failingRedis) Set(ctx context.Context, key string, value any, expiration time.Duration) *redis.StatusCmd {
	return f.inner.Set(ctx, key, value, expiration)
}

func (f *failingRedis) Exists(ctx context.Context, keys ...string) *redis.IntCmd {
	return f.inner.Exists(ctx, keys...)
}

func (f *failingRedis) Del(ctx context.Context, keys ...string) *redis.IntCmd {
	return f.inner.Del(ctx, keys...)
}

func (f *failingRedis) LPush(ctx context.Context, key string, values ...any) *redis.IntCmd {
	f.mu.Lock()
	shouldFail := f.failLPush
	if shouldFail {
		f.failLPush = false
	}
	f.mu.Unlock()
	if shouldFail {
		cmd := redis.NewIntCmd(ctx, "lpush", key)
		cmd.SetErr(errors.New("injected lpush failure"))
		return cmd
	}
	return f.inner.LPush(ctx, key, values...)
}

func (f *failingRedis) RPop(ctx context.Context, key string) *redis.StringCmd {
	return f.inner.RPop(ctx, key)
}

func TestRun_SessionIDReadFailsAfterInit_ReturnsErr(t *testing.T) {
	mr, r, svc, cfg := runTestSetup(t)
	mr.Set(cfg.SessionIDKey, "sess-1") // Exists check passes.

	fr := &failingRedis{inner: r, failGetKey: cfg.SessionIDKey}

	err := run(context.Background(), cfg, fr, svc)
	if err == nil {
		t.Fatal("want error when session ID Get fails right after init")
	}
	if !strings.Contains(err.Error(), "failed to read session ID") {
		t.Errorf("err=%v missing expected wrap", err)
	}
}

func TestRun_InitialResolveVoiceIDFails_ReturnsErr(t *testing.T) {
	mr, r, svc, cfg := runTestSetup(t)

	// Empty voices file: load succeeds after we point at a non-empty temp file,
	// then we replace it and force resolveVoiceID to fail. Simpler: rewrite the
	// voices file with a voice that isn't in session_info and clear the voices
	// map by using an empty file? loadVoices rejects empty maps.
	//
	// The cleanest way to force initial resolveVoiceID to error is to kill
	// Redis after publishVoices + waitForSessionInit but before the Get on
	// SessionInfoKey. We do that via a failingRedis that fails a single Get
	// on SessionInfoKey.
	mr.Set(cfg.SessionIDKey, "sess-1")
	mr.Set(cfg.SessionInfoKey, `{"as_of":1,"voice_id":"only","prompt_id":"p"}`)

	fr := &failingRedis{inner: r, failGetKey: cfg.SessionInfoKey}

	err := run(context.Background(), cfg, fr, svc)
	if err == nil {
		t.Fatal("want error when initial resolveVoiceID fails")
	}
	if !strings.Contains(err.Error(), "failed to read session info") {
		t.Errorf("err=%v missing expected wrap", err)
	}
}

func TestRun_WarmupTTSFails_ReturnsErr(t *testing.T) {
	mr, r, _, cfg := runTestSetup(t)
	mr.Set(cfg.SessionIDKey, "sess-1")
	mr.Set(cfg.SessionInfoKey, `{"as_of":1,"voice_id":"only","prompt_id":"p"}`)

	svc := &fakeTTS{err: errors.New("auth failed")}

	err := run(context.Background(), cfg, r, svc)
	if err == nil {
		t.Fatal("want error when warmup TTS fails")
	}
	if !strings.Contains(err.Error(), "warmup generation failed") {
		t.Errorf("err=%v missing expected wrap", err)
	}
}

func TestRun_GenerateErrorInLoop_ContinuesWithoutExit(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping slow test in -short mode")
	}
	mr, r, _, cfg := runTestSetup(t)
	mr.Set(cfg.SessionIDKey, "sess-1")
	mr.Set(cfg.SessionInfoKey, `{"as_of":1,"voice_id":"only","prompt_id":"p"}`)
	mr.Set(cfg.RedisTriggerKeyName, "true")

	// This fake succeeds for warmup, then fails on the FIRST loop call, then
	// succeeds again. This exercises the "generateSpeech error in loop → log
	// and continue" branch and proves the loop keeps running afterward.
	svc := &sequencedFakeTTS{
		responses: []fakeTTSResp{
			{data: int16BytesLE(0)},           // warmup
			{err: errors.New("transient")},   // first loop call — fails
			{data: int16BytesLE(1, 2, 3, 4)}, // recovery
		},
	}

	inputQ := inputQueueName(cfg, "sess-1")
	// Push two chunks so the loop iterates at least twice.
	_, _ = r.LPush(context.Background(), inputQ, "will-fail").Result()
	_, _ = r.LPush(context.Background(), inputQ, "will-succeed").Result()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx, cfg, r, svc) }()

	outputQ := outputQueueName(cfg, "sess-1")
	deadline := time.After(3 * time.Second)
	for {
		n, _ := r.LLen(context.Background(), outputQ).Result()
		if n >= 1 {
			break
		}
		select {
		case <-deadline:
			cancel()
			t.Fatal("timeout waiting for recovery after transient TTS error")
		case <-time.After(50 * time.Millisecond):
		}
	}

	cancel()
	_ = waitForRunExit(t, done, 2*time.Second)

	svc.mu.Lock()
	defer svc.mu.Unlock()
	if svc.n < 3 {
		t.Errorf("TTS calls=%d want >=3 (warmup + fail + recovery)", svc.n)
	}
}

func TestRun_LPushFailure_ContinuesWithoutExit(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping slow test in -short mode")
	}
	mr, r, svc, cfg := runTestSetup(t)
	mr.Set(cfg.SessionIDKey, "sess-1")
	mr.Set(cfg.SessionInfoKey, `{"as_of":1,"voice_id":"only","prompt_id":"p"}`)
	mr.Set(cfg.RedisTriggerKeyName, "true")

	fr := &failingRedis{inner: r, failLPush: true}

	inputQ := inputQueueName(cfg, "sess-1")
	_, _ = r.LPush(context.Background(), inputQ, "first").Result()
	_, _ = r.LPush(context.Background(), inputQ, "second").Result()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx, cfg, fr, svc) }()

	// After the injected LPush failure clears itself, the next chunk should
	// still land in the output queue — proving the loop recovered.
	outputQ := outputQueueName(cfg, "sess-1")
	deadline := time.After(3 * time.Second)
	for {
		n, _ := r.LLen(context.Background(), outputQ).Result()
		if n >= 1 {
			break
		}
		select {
		case <-deadline:
			cancel()
			t.Fatal("timeout waiting for recovery after injected LPush failure")
		case <-time.After(50 * time.Millisecond):
		}
	}

	cancel()
	_ = waitForRunExit(t, done, 2*time.Second)
}

func TestRun_TriggerReadError_DoesNotExit(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping slow test in -short mode")
	}
	mr, r, svc, cfg := runTestSetup(t)
	mr.Set(cfg.SessionIDKey, "sess-1")
	mr.Set(cfg.SessionInfoKey, `{"as_of":1,"voice_id":"only","prompt_id":"p"}`)
	mr.Set(cfg.RedisTriggerKeyName, "true")

	// Fail one Get on the trigger key. The loop's first iteration reads
	// trigger → error → log + sleep 1s → next iteration recovers and proceeds.
	fr := &failingRedis{inner: r, failGetKey: cfg.RedisTriggerKeyName}

	_, _ = r.LPush(context.Background(), inputQueueName(cfg, "sess-1"), "hello").Result()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx, cfg, fr, svc) }()

	outputQ := outputQueueName(cfg, "sess-1")
	deadline := time.After(3 * time.Second)
	for {
		n, _ := r.LLen(context.Background(), outputQ).Result()
		if n >= 1 {
			break
		}
		select {
		case <-deadline:
			cancel()
			t.Fatal("timeout waiting for recovery after trigger Get failure")
		case <-time.After(50 * time.Millisecond):
		}
	}

	cancel()
	_ = waitForRunExit(t, done, 3*time.Second)
}

func TestRun_SessionIDChangesAfterTrigger_UpdatesQueues(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping slow test in -short mode")
	}
	mr, r, svc, cfg := runTestSetup(t)
	mr.Set(cfg.SessionIDKey, "sess-A")
	mr.Set(cfg.SessionInfoKey, `{"as_of":1,"voice_id":"only","prompt_id":"p"}`)
	// Trigger unset → first iteration hits the trigger-false branch,
	// clears the output queue, then waits.

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx, cfg, r, svc) }()

	// Wait until the run loop is parked in waitForTrigger. Once we see the
	// "Waiting for trigger key..." status, swap the session ID and flip the
	// trigger. The loop should re-read session ID, notice it changed, and
	// consume from the NEW session's input queue.
	deadline := time.After(3 * time.Second)
	for {
		raw, err := mr.Get(cfg.RedisStatusOutputName)
		if err == nil && strings.Contains(raw, "Waiting for trigger") {
			break
		}
		select {
		case <-deadline:
			cancel()
			t.Fatal("run never entered waiting-for-trigger state")
		case <-time.After(25 * time.Millisecond):
		}
	}

	mr.Set(cfg.SessionIDKey, "sess-B")
	// Queue an item on sess-B's input queue so we can prove the loop
	// switched over.
	_, _ = r.LPush(context.Background(), inputQueueName(cfg, "sess-B"), "on-B").Result()
	mr.Set(cfg.RedisTriggerKeyName, "true")

	outputQB := outputQueueName(cfg, "sess-B")
	deadline = time.After(4 * time.Second)
	for {
		n, _ := r.LLen(context.Background(), outputQB).Result()
		if n >= 1 {
			break
		}
		select {
		case <-deadline:
			cancel()
			t.Fatal("loop never produced audio on sess-B output queue")
		case <-time.After(50 * time.Millisecond):
		}
	}

	cancel()
	_ = waitForRunExit(t, done, 3*time.Second)
}

func TestRun_ResolveVoiceIDFailsAfterTrigger_ContinuesWithoutExit(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping slow test in -short mode")
	}
	mr, r, svc, cfg := runTestSetup(t)
	mr.Set(cfg.SessionIDKey, "sess-1")
	mr.Set(cfg.SessionInfoKey, `{"as_of":1,"voice_id":"only","prompt_id":"p"}`)

	// Wrap so the FIRST post-trigger Get on SessionInfoKey fails but subsequent
	// ones succeed — the loop should log + continue and then recover.
	fr := &failingRedis{inner: r}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx, cfg, fr, svc) }()

	// Wait for the run loop to reach the waiting-for-trigger state.
	deadline := time.After(3 * time.Second)
	for {
		raw, err := mr.Get(cfg.RedisStatusOutputName)
		if err == nil && strings.Contains(raw, "Waiting for trigger") {
			break
		}
		select {
		case <-deadline:
			cancel()
			t.Fatal("run never entered waiting-for-trigger state")
		case <-time.After(25 * time.Millisecond):
		}
	}

	// Arm the failing SessionInfo Get to fire on the post-trigger re-check,
	// then flip the trigger.
	fr.mu.Lock()
	fr.failGetKey = cfg.SessionInfoKey
	fr.mu.Unlock()
	mr.Set(cfg.RedisTriggerKeyName, "true")

	// The re-check errors, the loop continues, and then eventually generates
	// audio for a chunk we push.
	_, _ = r.LPush(context.Background(), inputQueueName(cfg, "sess-1"), "hello").Result()

	outputQ := outputQueueName(cfg, "sess-1")
	deadline = time.After(4 * time.Second)
	for {
		n, _ := r.LLen(context.Background(), outputQ).Result()
		if n >= 1 {
			break
		}
		select {
		case <-deadline:
			cancel()
			t.Fatal("loop never recovered from resolveVoiceID failure")
		case <-time.After(50 * time.Millisecond):
		}
	}

	cancel()
	_ = waitForRunExit(t, done, 3*time.Second)
}

// ---------------------------------------------------------------------------
// sequencedFakeTTS: yields responses/errors in a fixed order for tests that
// need warmup-then-fail-then-succeed behavior.
// ---------------------------------------------------------------------------

type fakeTTSResp struct {
	data []byte
	err  error
}

type sequencedFakeTTS struct {
	mu        sync.Mutex
	n         int
	responses []fakeTTSResp
}

func (s *sequencedFakeTTS) Generate(_ context.Context, _ *tts.Request) (*tts.Response, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	idx := s.n
	s.n++
	// Last response repeats for any calls beyond the scripted list.
	if idx >= len(s.responses) {
		idx = len(s.responses) - 1
	}
	resp := s.responses[idx]
	if resp.err != nil {
		return nil, resp.err
	}
	return &tts.Response{Audio: bytes.NewReader(resp.data)}, nil
}
