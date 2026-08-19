package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
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

func newTestConfig(t *testing.T, mr *miniredis.Miniredis) *config {
	t.Helper()
	return &config{
		RedisAddress:    mr.Host(),
		RedisPort:       mustAtoi(t, mr.Port()),
		InputQueue:      "queues:generated_audio_bytes",
		IcecastAddress:  "icecast.example",
		IcecastPort:     8069,
		IcecastPassword: "hunter2",
		Delay:           0,
	}
}

func newRedisClient(cfg *config) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("%s:%d", cfg.RedisAddress, cfg.RedisPort),
	})
}

// fakeSink records everything written so we can assert on it. It also allows
// injecting errors on write/flush/close for the error-path tests.
type fakeSink struct {
	mu         sync.Mutex
	buf        bytes.Buffer
	writes     int
	flushes    int
	closes     int
	writeErr   error
	flushErr   error
	closeErr   error
	writeAfter int // if >0, writeErr fires only after the Nth Write
}

func (f *fakeSink) Write(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.writes++
	if f.writeErr != nil && (f.writeAfter == 0 || f.writes > f.writeAfter) {
		return 0, f.writeErr
	}
	return f.buf.Write(p)
}

func (f *fakeSink) Flush() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.flushes++
	return f.flushErr
}

func (f *fakeSink) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closes++
	return f.closeErr
}

func (f *fakeSink) written() []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]byte(nil), f.buf.Bytes()...)
}

func (f *fakeSink) counts() (writes, flushes, closes int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.writes, f.flushes, f.closes
}

// ---------------------------------------------------------------------------
// constructSilence
// ---------------------------------------------------------------------------

func TestConstructSilence_LengthMatchesDurationAndChannels(t *testing.T) {
	got := constructSilence(0.1, 24000, 1)
	// 0.1s * 24000 samples/s * 1 channel * 4 bytes/float32 = 9600 bytes
	if len(got) != 9600 {
		t.Errorf("len=%d want 9600", len(got))
	}
	for i, b := range got {
		if b != 0 {
			t.Errorf("silence[%d]=%d want 0", i, b)
			break
		}
	}
}

func TestConstructSilence_ZeroDuration(t *testing.T) {
	got := constructSilence(0, 24000, 1)
	if len(got) != 0 {
		t.Errorf("len=%d want 0", len(got))
	}
}

func TestConstructSilence_MultichannelScalesLength(t *testing.T) {
	got := constructSilence(0.1, 24000, 2)
	// Stereo doubles it.
	if len(got) != 9600*2 {
		t.Errorf("len=%d want %d", len(got), 9600*2)
	}
}

// ---------------------------------------------------------------------------
// padToFrameSize
// ---------------------------------------------------------------------------

func TestPadToFrameSize_ExactMultipleAddsOnlyExtraFrames(t *testing.T) {
	// 2 frames of data = 2048 samples = 8192 bytes.
	in := make([]byte, 2*1024*4)
	out := padToFrameSize(in, 1024, 10)
	want := len(in) + 10*1024*4
	if len(out) != want {
		t.Errorf("len=%d want %d", len(out), want)
	}
	// Data prefix preserved.
	if !bytes.Equal(out[:len(in)], in) {
		t.Error("prefix modified")
	}
	// Trailing bytes are all zero.
	for _, b := range out[len(in):] {
		if b != 0 {
			t.Fatal("trailing bytes not zero")
		}
	}
}

func TestPadToFrameSize_PartialFrameRoundsUp(t *testing.T) {
	// 1.5 frames worth of samples: 1536 samples = 6144 bytes.
	in := make([]byte, 1536*4)
	out := padToFrameSize(in, 1024, 10)
	// Want 2 frames + 10 extra = 12 frames = 12288 samples = 49152 bytes.
	if len(out) != 12*1024*4 {
		t.Errorf("len=%d want %d", len(out), 12*1024*4)
	}
}

func TestPadToFrameSize_EmptyInputStillGetsExtraFrames(t *testing.T) {
	out := padToFrameSize(nil, 1024, 10)
	if len(out) != 10*1024*4 {
		t.Errorf("len=%d want %d", len(out), 10*1024*4)
	}
}

func TestPadToFrameSize_ZeroExtraExactMultipleIsNoOp(t *testing.T) {
	in := make([]byte, 1024*4)
	out := padToFrameSize(in, 1024, 0)
	if len(out) != len(in) {
		t.Errorf("len=%d want %d", len(out), len(in))
	}
}

func TestPadToFrameSize_ZeroFrameSizeReturnsInputUnchanged(t *testing.T) {
	in := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	out := padToFrameSize(in, 0, 10)
	if !bytes.Equal(out, in) {
		t.Errorf("out=%v want %v", out, in)
	}
}

func TestPadToFrameSize_NegativeFrameSizeReturnsInputUnchanged(t *testing.T) {
	in := []byte{1, 2, 3, 4}
	out := padToFrameSize(in, -1, 10)
	if !bytes.Equal(out, in) {
		t.Errorf("out=%v want %v", out, in)
	}
}

func TestPadToFrameSize_MisalignedInputTruncated(t *testing.T) {
	// 5 bytes → not a float32 multiple. Truncate to 4, then pad up to
	// a frame + extra.
	in := []byte{1, 2, 3, 4, 99}
	out := padToFrameSize(in, 1024, 10)
	// After truncation we have 1 sample, then pad to 11 * 1024 samples.
	want := 11 * 1024 * 4
	if len(out) != want {
		t.Errorf("len=%d want %d", len(out), want)
	}
	// The first 4 bytes should be the preserved sample.
	if !bytes.Equal(out[:4], []byte{1, 2, 3, 4}) {
		t.Errorf("truncation did not preserve aligned prefix, got=%v", out[:4])
	}
}

// ---------------------------------------------------------------------------
// playAudio
// ---------------------------------------------------------------------------

func TestPlayAudio_WritesPaddedBytesAndFlushes(t *testing.T) {
	// 100 samples input → padded to a multiple of frameSize plus 10 trailing.
	pcm := float32BytesLE(make([]float32, 100))
	sink := &fakeSink{}
	if err := playAudio(sink, pcm); err != nil {
		t.Fatalf("err=%v", err)
	}
	writes, flushes, _ := sink.counts()
	if writes != 1 || flushes != 1 {
		t.Errorf("writes=%d flushes=%d want 1/1", writes, flushes)
	}
	got := sink.written()
	// Must be a multiple of frameSize*4 and at least the trailing silence.
	if len(got)%(frameSize*4) != 0 {
		t.Errorf("written len=%d not a frame multiple", len(got))
	}
	if len(got) < trailingSilenceFrames*frameSize*4 {
		t.Errorf("written len=%d too short for trailing silence", len(got))
	}
}

func TestPlayAudio_WriteErrorWrapped(t *testing.T) {
	sink := &fakeSink{writeErr: errors.New("pipe closed")}
	err := playAudio(sink, float32BytesLE(make([]float32, 10)))
	if err == nil {
		t.Fatal("want error")
	}
	if !errors.Is(err, sink.writeErr) {
		t.Errorf("err=%v want wraps pipe closed", err)
	}
}

func TestPlayAudio_FlushErrorWrapped(t *testing.T) {
	sink := &fakeSink{flushErr: errors.New("flush boom")}
	err := playAudio(sink, float32BytesLE(make([]float32, 10)))
	if err == nil {
		t.Fatal("want error")
	}
	if !errors.Is(err, sink.flushErr) {
		t.Errorf("err=%v want wraps flush boom", err)
	}
}

// ---------------------------------------------------------------------------
// sleepWithCtx
// ---------------------------------------------------------------------------

func TestSleepWithCtx_ZeroReturnsImmediately(t *testing.T) {
	start := time.Now()
	if err := sleepWithCtx(context.Background(), 0); err != nil {
		t.Fatalf("err=%v", err)
	}
	if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
		t.Errorf("elapsed=%v want ~0", elapsed)
	}
}

func TestSleepWithCtx_NegativeReturnsImmediately(t *testing.T) {
	if err := sleepWithCtx(context.Background(), -time.Second); err != nil {
		t.Fatalf("err=%v", err)
	}
}

func TestSleepWithCtx_ZeroWithCanceledCtxReturnsCtxErr(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := sleepWithCtx(ctx, 0)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err=%v want context.Canceled", err)
	}
}

func TestSleepWithCtx_Elapses(t *testing.T) {
	start := time.Now()
	if err := sleepWithCtx(context.Background(), 50*time.Millisecond); err != nil {
		t.Fatalf("err=%v", err)
	}
	if elapsed := time.Since(start); elapsed < 40*time.Millisecond {
		t.Errorf("elapsed=%v want >=40ms", elapsed)
	}
}

func TestSleepWithCtx_CtxCancelBeforeTimer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	err := sleepWithCtx(ctx, 10*time.Second)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err=%v want context.Canceled", err)
	}
}

// ---------------------------------------------------------------------------
// runLoop
// ---------------------------------------------------------------------------

func TestRunLoop_ContextCanceledBeforeStart_Returns(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := newTestConfig(t, mr)
	r := newRedisClient(cfg)
	t.Cleanup(func() { _ = r.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	sink := &fakeSink{}
	err := runLoop(ctx, cfg, r, sink)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err=%v want context.Canceled", err)
	}
	if w, _, _ := sink.counts(); w != 0 {
		t.Errorf("writes=%d want 0", w)
	}
}

func TestRunLoop_ConsumesQueuedChunksAndCancels(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := newTestConfig(t, mr)
	r := newRedisClient(cfg)
	t.Cleanup(func() { _ = r.Close() })

	// Enqueue two chunks; RPop drains them.
	chunk1 := float32BytesLE([]float32{0.1, 0.2, 0.3})
	chunk2 := float32BytesLE([]float32{-0.5, 0.5})
	_, _ = r.LPush(context.Background(), cfg.InputQueue, chunk2, chunk1).Result()

	sink := &fakeSink{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runLoop(ctx, cfg, r, sink) }()

	// Poll until both chunks are consumed and at least one silence write has
	// happened (the queue empties out, so hasWrittenOnce is true → silence
	// injection kicks in on the next poll).
	deadline := time.After(3 * time.Second)
	for {
		w, _, _ := sink.counts()
		if w >= 3 {
			break
		}
		select {
		case <-deadline:
			cancel()
			t.Fatalf("timeout; writes=%d", w)
		case <-time.After(20 * time.Millisecond):
		}
	}

	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("err=%v want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("runLoop did not exit after cancel")
	}
}

func TestRunLoop_NoWritesUntilFirstMessage(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := newTestConfig(t, mr)
	r := newRedisClient(cfg)
	t.Cleanup(func() { _ = r.Close() })

	// Queue stays empty for the whole test.
	sink := &fakeSink{}
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	err := runLoop(ctx, cfg, r, sink)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err=%v want DeadlineExceeded", err)
	}
	if w, _, _ := sink.counts(); w != 0 {
		t.Errorf("writes=%d want 0 before any real chunk", w)
	}
}

func TestRunLoop_WriteErrorSurfaces(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := newTestConfig(t, mr)
	r := newRedisClient(cfg)
	t.Cleanup(func() { _ = r.Close() })

	_, _ = r.LPush(context.Background(), cfg.InputQueue, float32BytesLE([]float32{1})).Result()

	sink := &fakeSink{writeErr: errors.New("pipe boom")}
	err := runLoop(context.Background(), cfg, r, sink)
	if err == nil {
		t.Fatal("want error")
	}
	if !errors.Is(err, sink.writeErr) {
		t.Errorf("err=%v want wraps pipe boom", err)
	}
}

func TestRunLoop_SilenceWriteErrorSurfaces(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := newTestConfig(t, mr)
	r := newRedisClient(cfg)
	t.Cleanup(func() { _ = r.Close() })

	// One real chunk to flip hasWrittenOnce, then subsequent Writes fail
	// on the silence injection.
	_, _ = r.LPush(context.Background(), cfg.InputQueue, float32BytesLE([]float32{1, 2, 3})).Result()

	sink := &fakeSink{writeErr: errors.New("silence boom"), writeAfter: 1}
	err := runLoop(context.Background(), cfg, r, sink)
	if err == nil {
		t.Fatal("want error")
	}
	if !errors.Is(err, sink.writeErr) {
		t.Errorf("err=%v want wraps silence boom", err)
	}
}

func TestRunLoop_RedisErrorSurfaces(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := newTestConfig(t, mr)
	r := newRedisClient(cfg)
	t.Cleanup(func() { _ = r.Close() })
	mr.Close()

	sink := &fakeSink{}
	err := runLoop(context.Background(), cfg, r, sink)
	if err == nil {
		t.Fatal("want error when Redis is unreachable")
	}
	if !bytes.Contains([]byte(err.Error()), []byte("failed to rpop")) {
		t.Errorf("err=%v missing expected text", err)
	}
}

// ctxCancelSink cancels the given context on its first Write after a real
// chunk is played. Used to exit runLoop deterministically after checking
// that a real chunk was written.
type ctxCancelSink struct {
	fakeSink
	cancel context.CancelFunc
}

func (c *ctxCancelSink) Write(p []byte) (int, error) {
	n, err := c.fakeSink.Write(p)
	c.cancel()
	return n, err
}

func TestRunLoop_DelayHonored(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := newTestConfig(t, mr)
	cfg.Delay = 50 * time.Millisecond
	r := newRedisClient(cfg)
	t.Cleanup(func() { _ = r.Close() })

	_, _ = r.LPush(context.Background(), cfg.InputQueue, float32BytesLE([]float32{1})).Result()

	ctx, cancel := context.WithCancel(context.Background())
	sink := &ctxCancelSink{cancel: cancel}
	start := time.Now()
	err := runLoop(ctx, cfg, r, sink)
	elapsed := time.Since(start)

	if !errors.Is(err, context.Canceled) {
		t.Errorf("err=%v want context.Canceled", err)
	}
	// The sink cancels immediately after Write, but the loop still hits the
	// Delay sleep on the way out. Since sleepWithCtx returns as soon as ctx
	// cancels, elapsed should be well under the 50ms delay.
	if elapsed > 500*time.Millisecond {
		t.Errorf("elapsed=%v; loop didn't respect cancel", elapsed)
	}
}

// ---------------------------------------------------------------------------
// float32BytesLE
// ---------------------------------------------------------------------------

func TestFloat32BytesLE_RoundTrips(t *testing.T) {
	in := []float32{0, 1, -1, 0.5, math.MaxFloat32}
	out := float32BytesLE(in)
	if len(out) != len(in)*4 {
		t.Fatalf("len=%d want %d", len(out), len(in)*4)
	}
	for i, want := range in {
		got := math.Float32frombits(binary.LittleEndian.Uint32(out[i*4 : i*4+4]))
		if got != want {
			t.Errorf("sample %d = %v want %v", i, got, want)
		}
	}
}

// ---------------------------------------------------------------------------
// ffmpegSink + newFFmpegSink
// ---------------------------------------------------------------------------

// withCommand swaps the package-level newCommand hook for the duration of a
// test, restoring it on cleanup.
func withCommand(t *testing.T, fn func(name string, args ...string) *exec.Cmd) {
	t.Helper()
	orig := newCommand
	newCommand = fn
	t.Cleanup(func() { newCommand = orig })
}

func TestFfmpegSink_WriteCloseHappyPath(t *testing.T) {
	// Substitute a trivial `cat` for ffmpeg. cat reads stdin and writes to
	// stdout — we can Close its stdin and Wait for it to exit normally.
	withCommand(t, func(_ string, _ ...string) *exec.Cmd {
		return exec.Command("cat")
	})

	cfg := &config{IcecastPassword: "x", IcecastAddress: "h", IcecastPort: 1}
	sink, err := newFFmpegSink(cfg)
	if err != nil {
		t.Fatalf("newFFmpegSink err=%v", err)
	}

	if _, err := sink.Write([]byte("hello\n")); err != nil {
		t.Errorf("Write err=%v", err)
	}
	if err := sink.Flush(); err != nil {
		t.Errorf("Flush err=%v", err)
	}
	if err := sink.Close(); err != nil {
		t.Errorf("Close err=%v", err)
	}
}

func TestFfmpegSink_StartFailure_Reported(t *testing.T) {
	// A command that doesn't exist causes cmd.Start() to fail.
	withCommand(t, func(_ string, _ ...string) *exec.Cmd {
		return exec.Command("/nonexistent/binary-that-cannot-be-executed")
	})

	cfg := &config{IcecastPassword: "x", IcecastAddress: "h", IcecastPort: 1}
	_, err := newFFmpegSink(cfg)
	if err == nil {
		t.Fatal("want error when ffmpeg binary missing")
	}
	if !bytes.Contains([]byte(err.Error()), []byte("start ffmpeg")) {
		t.Errorf("err=%v missing 'start ffmpeg'", err)
	}
}

func TestFfmpegSink_StdinPipeFailure_Reported(t *testing.T) {
	// If cmd already has Stdin set, StdinPipe fails.
	withCommand(t, func(_ string, _ ...string) *exec.Cmd {
		c := exec.Command("cat")
		c.Stdin = bytes.NewReader(nil)
		return c
	})

	cfg := &config{IcecastPassword: "x", IcecastAddress: "h", IcecastPort: 1}
	_, err := newFFmpegSink(cfg)
	if err == nil {
		t.Fatal("want error when StdinPipe fails")
	}
	if !bytes.Contains([]byte(err.Error()), []byte("open ffmpeg stdin pipe")) {
		t.Errorf("err=%v missing 'open ffmpeg stdin pipe'", err)
	}
}

func TestFfmpegSink_CloseStdinError_Reported(t *testing.T) {
	// Build a real sink so cmd.Wait() has something to wait on, then swap in
	// a stdin whose Close() errors.
	withCommand(t, func(_ string, _ ...string) *exec.Cmd {
		return exec.Command("cat")
	})
	cfg := &config{IcecastPassword: "x", IcecastAddress: "h", IcecastPort: 1}
	sink, err := newFFmpegSink(cfg)
	if err != nil {
		t.Fatalf("newFFmpegSink err=%v", err)
	}
	// Replace stdin with a WriteCloser whose Close always errors. We still
	// need to close the real pipe so cat exits — do that after.
	realStdin := sink.stdin
	sink.stdin = &errCloseWriter{inner: realStdin}

	err = sink.Close()
	if err == nil {
		// Make sure we don't leak cat: close the real pipe and wait.
		_ = realStdin.Close()
		_ = sink.cmd.Wait()
		t.Fatal("want Close to surface stdin error")
	}
	// Reap the real process now that the stdin close error was reported.
	_ = realStdin.Close()
	_ = sink.cmd.Wait()
}

// TestNewCommand_DefaultReturnsExecCmd exercises the default newCommand hook
// so its body is covered without needing ffmpeg on PATH.
func TestNewCommand_DefaultReturnsExecCmd(t *testing.T) {
	cmd := newCommand("cat", "-u")
	if cmd == nil {
		t.Fatal("newCommand returned nil")
	}
	if cmd.Path == "" {
		t.Errorf("cmd.Path empty")
	}
	if got := cmd.Args; len(got) < 2 || got[1] != "-u" {
		t.Errorf("cmd.Args=%v want [<path>, -u]", got)
	}
}

// errCloseWriter wraps a WriteCloser and always returns an error on Close.
type errCloseWriter struct{ inner io.WriteCloser }

func (e *errCloseWriter) Write(p []byte) (int, error) { return e.inner.Write(p) }
func (e *errCloseWriter) Close() error                { return errors.New("stdin close boom") }

// ---------------------------------------------------------------------------
// realMain
// ---------------------------------------------------------------------------

// setEnv wraps t.Setenv over multiple KV pairs, restoring them on cleanup.
func setEnv(t *testing.T, kv map[string]string) {
	t.Helper()
	for k, v := range kv {
		t.Setenv(k, v)
	}
}

func TestRealMain_MissingRequiredEnvVar_ReturnsErr(t *testing.T) {
	// t.Setenv can only set values, not unset them, so scope this to a
	// subprocess-safe env manipulation: set to a bogus name via Unsetenv
	// after t.Setenv snapshots the original.
	t.Setenv("ICECAST_PASSWORD", "placeholder")
	if err := os.Unsetenv("ICECAST_PASSWORD"); err != nil {
		t.Fatalf("unsetenv: %v", err)
	}
	err := realMain(context.Background())
	if err == nil {
		t.Fatal("want error when ICECAST_PASSWORD is missing")
	}
	if !bytes.Contains([]byte(err.Error()), []byte("failed to parse env config")) {
		t.Errorf("err=%v missing expected text", err)
	}
}

func TestRealMain_FFmpegStartFailure_ReturnsErr(t *testing.T) {
	mr := miniredis.RunT(t)
	setEnv(t, map[string]string{
		"ICECAST_PASSWORD": "hunter2",
		"REDIS_ADDRESS":    mr.Host(),
		"REDIS_PORT":       mr.Port(),
	})
	withCommand(t, func(_ string, _ ...string) *exec.Cmd {
		return exec.Command("/nonexistent/binary")
	})

	err := realMain(context.Background())
	if err == nil {
		t.Fatal("want error when ffmpeg fails to start")
	}
	if !bytes.Contains([]byte(err.Error()), []byte("failed to start ffmpeg")) {
		t.Errorf("err=%v missing expected text", err)
	}
}

func TestRealMain_EndToEnd_WithCatAsFakeFFmpeg(t *testing.T) {
	mr := miniredis.RunT(t)
	setEnv(t, map[string]string{
		"ICECAST_PASSWORD": "hunter2",
		"REDIS_ADDRESS":    mr.Host(),
		"REDIS_PORT":       mr.Port(),
	})
	withCommand(t, func(_ string, _ ...string) *exec.Cmd {
		return exec.Command("cat")
	})

	// Push one chunk so runLoop has work to do before ctx cancels.
	mr.Lpush("queues:generated_audio_bytes", string(float32BytesLE([]float32{1, 2, 3})))

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	err := realMain(ctx)
	if err == nil {
		t.Fatal("want realMain to return runLoop's ctx error, not nil")
	}
	if !bytes.Contains([]byte(err.Error()), []byte("icecast_audio_pusher exited with error")) {
		t.Errorf("err=%v missing expected wrap", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err=%v want wraps DeadlineExceeded", err)
	}
}

// TestRealMain_SinkCloseHappyPath drives realMain to completion with a cat
// stand-in for ffmpeg; the deferred sink.Close() runs on the happy path.
func TestRealMain_SinkCloseHappyPath(t *testing.T) {
	mr := miniredis.RunT(t)
	setEnv(t, map[string]string{
		"ICECAST_PASSWORD": "hunter2",
		"REDIS_ADDRESS":    mr.Host(),
		"REDIS_PORT":       mr.Port(),
	})
	withCommand(t, func(_ string, _ ...string) *exec.Cmd {
		return exec.Command("cat")
	})

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_ = realMain(ctx)
}

// TestRealMain_SinkCloseErrorLogged exercises the deferred close-error log
// branch. We substitute a command that exits with non-zero for ffmpeg, so
// cmd.Wait() (called inside sink.Close()) returns an *exec.ExitError.
func TestRealMain_SinkCloseErrorLogged(t *testing.T) {
	mr := miniredis.RunT(t)
	setEnv(t, map[string]string{
		"ICECAST_PASSWORD": "hunter2",
		"REDIS_ADDRESS":    mr.Host(),
		"REDIS_PORT":       mr.Port(),
	})
	withCommand(t, func(_ string, _ ...string) *exec.Cmd {
		// `sh -c "cat; exit 1"` drains stdin then exits non-zero. Close()
		// closes stdin (cat exits), then Wait() surfaces the shell's
		// non-zero exit as an error.
		return exec.Command("sh", "-c", "cat; exit 1")
	})

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_ = realMain(ctx)
}

// withRunLoopFn swaps the package-level runLoopFn hook for a test.
func withRunLoopFn(t *testing.T, fn func(context.Context, *config, redisClient, audioSink) error) {
	t.Helper()
	orig := runLoopFn
	runLoopFn = fn
	t.Cleanup(func() { runLoopFn = orig })
}

// withPanicOnErr swaps the package-level panicOnErr exit hook for a test.
func withPanicOnErr(t *testing.T, fn func(error)) {
	t.Helper()
	orig := panicOnErr
	panicOnErr = fn
	t.Cleanup(func() { panicOnErr = orig })
}

// TestRealMain_RunLoopReturnsNilPropagates covers the tail `return nil`
// branch by force-swapping runLoopFn to return nil.
func TestRealMain_RunLoopReturnsNilPropagates(t *testing.T) {
	mr := miniredis.RunT(t)
	setEnv(t, map[string]string{
		"ICECAST_PASSWORD": "hunter2",
		"REDIS_ADDRESS":    mr.Host(),
		"REDIS_PORT":       mr.Port(),
	})
	withCommand(t, func(_ string, _ ...string) *exec.Cmd { return exec.Command("cat") })
	withRunLoopFn(t, func(_ context.Context, _ *config, _ redisClient, _ audioSink) error {
		return nil
	})

	if err := realMain(context.Background()); err != nil {
		t.Fatalf("realMain err=%v want nil", err)
	}
}

// TestMain_ExitHookInvokedOnError swaps the exit hook to capture the error
// rather than terminating the process.
func TestMain_ExitHookInvokedOnError(t *testing.T) {
	// Force realMain to fail by unsetting the required env var.
	t.Setenv("ICECAST_PASSWORD", "placeholder")
	if err := os.Unsetenv("ICECAST_PASSWORD"); err != nil {
		t.Fatalf("unsetenv: %v", err)
	}

	var captured error
	withPanicOnErr(t, func(err error) { captured = err })

	main()

	if captured == nil {
		t.Fatal("expected panicOnErr to be invoked with the realMain error")
	}
	if !bytes.Contains([]byte(captured.Error()), []byte("failed to parse env config")) {
		t.Errorf("captured=%v missing expected text", captured)
	}
}

// TestPanicOnErr_DefaultPanics invokes the default panicOnErr hook directly
// and recovers, so the body of the default definition itself is covered.
func TestPanicOnErr_DefaultPanics(t *testing.T) {
	orig := panicOnErr
	t.Cleanup(func() { panicOnErr = orig })

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("default panicOnErr did not panic")
		}
	}()
	panicOnErr(errors.New("synthetic"))
}

// TestMain_ExitHookNotInvokedOnSuccess covers the branch where realMain
// returns nil and main() falls through without invoking the exit hook.
func TestMain_ExitHookNotInvokedOnSuccess(t *testing.T) {
	mr := miniredis.RunT(t)
	setEnv(t, map[string]string{
		"ICECAST_PASSWORD": "hunter2",
		"REDIS_ADDRESS":    mr.Host(),
		"REDIS_PORT":       mr.Port(),
	})
	withCommand(t, func(_ string, _ ...string) *exec.Cmd { return exec.Command("cat") })
	withRunLoopFn(t, func(_ context.Context, _ *config, _ redisClient, _ audioSink) error {
		return nil
	})

	invoked := false
	withPanicOnErr(t, func(err error) { invoked = true })

	main()

	if invoked {
		t.Fatal("panicOnErr invoked on happy path")
	}
}
