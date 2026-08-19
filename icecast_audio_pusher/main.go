package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os/exec"
	"time"

	"github.com/caarlos0/env/v11"
	redispkg "github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

const (
	sampleRate            = 24000
	channels              = 1
	frameSize             = 1024
	trailingSilenceFrames = 10
	idleSilenceSec        = 0.1
	pollInterval          = 500 * time.Millisecond
)

// redisClient is the subset of *redis.Client the pusher uses. Kept as an
// interface so tests can swap in a fake. *redis.Client satisfies this
// implicitly.
type redisClient interface {
	RPop(ctx context.Context, key string) *redispkg.StringCmd
}

// audioSink accepts float32 PCM byte payloads and flushes them. In production
// this wraps ffmpeg's stdin; tests can supply an in-memory writer to observe
// what would have been sent.
type audioSink interface {
	Write(p []byte) (int, error)
	Flush() error
	Close() error
}

type config struct {
	RedisAddress    string        `env:"REDIS_ADDRESS" envDefault:"localhost"`
	RedisPort       int           `env:"REDIS_PORT" envDefault:"6379"`
	InputQueue      string        `env:"REDIS_KEY_QUEUES_GENERATED_AUDIO_BYTES" envDefault:"queues:generated_audio_bytes"`
	IcecastAddress  string        `env:"ICECAST_ADDRESS" envDefault:"localhost"`
	IcecastPort     int           `env:"ICECAST_PORT" envDefault:"8069"`
	IcecastPassword string        `env:"ICECAST_PASSWORD,required"`
	Delay           time.Duration `env:"DELAY" envDefault:"0s"`
}

// constructSilence returns duration seconds of float32 PCM zeros at the given
// sample rate.
func constructSilence(durationSec float64, sr, ch int) []byte {
	nSamples := int(float64(sr) * durationSec)
	total := nSamples * ch
	// zeros as float32 is just a zero byte slice
	return make([]byte, total*4)
}

// padToFrameSize rounds pcm up to a multiple of frameSize float32 samples and
// then appends an extra `extraFrames` frames of silence. The trailing silence
// forces the encoder to flush the last real frame instead of holding it in
// its lookahead buffer.
func padToFrameSize(pcm []byte, fs, extraFrames int) []byte {
	if fs <= 0 {
		return pcm
	}
	if len(pcm)%4 != 0 {
		// Guard against callers passing non-float32-aligned data. Round down
		// to the nearest float32 boundary to avoid producing garbage samples.
		pcm = pcm[:len(pcm)-len(pcm)%4]
	}
	samples := len(pcm) / 4
	remainder := samples % fs
	padSamples := 0
	if remainder != 0 {
		padSamples = fs - remainder
	}
	padSamples += extraFrames * fs
	if padSamples == 0 {
		return pcm
	}
	out := make([]byte, len(pcm)+padSamples*4)
	copy(out, pcm)
	return out
}

// playAudio pads the given PCM chunk and writes it through the audio sink.
func playAudio(sink audioSink, pcm []byte) error {
	padded := padToFrameSize(pcm, frameSize, trailingSilenceFrames)
	if _, err := sink.Write(padded); err != nil {
		return fmt.Errorf("write to audio sink: %w", err)
	}
	if err := sink.Flush(); err != nil {
		return fmt.Errorf("flush audio sink: %w", err)
	}
	return nil
}

// sleepWithCtx sleeps for d, returning early with ctx.Err() if ctx is done.
// A zero or negative duration returns immediately (still respecting ctx).
func sleepWithCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// runLoopFn is the pump function used by realMain. It's a var so tests can
// swap it to force a specific return value.
var runLoopFn = runLoop

// runLoop is the main pump: pop the next float32 PCM chunk from Redis and
// stream it into the audio sink, injecting short silences during idle so the
// icecast client stays connected.
func runLoop(ctx context.Context, cfg *config, r redisClient, sink audioSink) error {
	hasWrittenOnce := false
	silence := constructSilence(idleSilenceSec, sampleRate, channels)

	log.Info().Msg("Waiting for audio bytes...")
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		next, err := r.RPop(ctx, cfg.InputQueue).Result()
		if err == nil {
			log.Info().Msgf("Playing %d bytes to ffmpeg", len(next))
			if err := playAudio(sink, []byte(next)); err != nil {
				return err
			}
			hasWrittenOnce = true
			if err := sleepWithCtx(ctx, cfg.Delay); err != nil {
				return err
			}
			continue
		}

		if !errors.Is(err, redispkg.Nil) {
			return fmt.Errorf("failed to rpop '%s': %w", cfg.InputQueue, err)
		}

		// Queue is empty. If we've streamed anything yet, keep the pipe warm
		// with a short burst of silence so ffmpeg/icecast don't stall.
		if hasWrittenOnce {
			if err := playAudio(sink, silence); err != nil {
				return err
			}
		}
		if err := sleepWithCtx(ctx, pollInterval); err != nil {
			return err
		}
	}
}

// ffmpegSink wraps a running ffmpeg process, exposing its stdin as an
// audioSink. Close() shuts stdin then waits for ffmpeg to exit.
type ffmpegSink struct {
	cmd   *exec.Cmd
	stdin io.WriteCloser
}

func (f *ffmpegSink) Write(p []byte) (int, error) { return f.stdin.Write(p) }
func (f *ffmpegSink) Flush() error                { return nil } // stdin is unbuffered
func (f *ffmpegSink) Close() error {
	if err := f.stdin.Close(); err != nil {
		return err
	}
	return f.cmd.Wait()
}

// newCommand is a swappable seam so tests can substitute a scripted binary
// for the real ffmpeg. Defaults to exec.Command.
var newCommand = func(name string, args ...string) *exec.Cmd {
	return exec.Command(name, args...)
}

// newFFmpegSink launches ffmpeg configured to read raw float32 PCM from
// stdin, encode to MP3, and stream to the configured icecast endpoint.
func newFFmpegSink(cfg *config) (*ffmpegSink, error) {
	url := fmt.Sprintf("icecast://source:%s@%s:%d/stream.mp3", cfg.IcecastPassword, cfg.IcecastAddress, cfg.IcecastPort)
	cmd := newCommand("ffmpeg",
		"-y",
		"-f", "f32le", "-ar", fmt.Sprintf("%d", sampleRate), "-ac", fmt.Sprintf("%d", channels), "-i", "pipe:0",
		"-c:a", "libmp3lame",
		"-b:a", "128k",
		"-content_type", "audio/mpeg",
		"-f", "mp3", url,
	)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("open ffmpeg stdin pipe: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start ffmpeg: %w", err)
	}
	return &ffmpegSink{cmd: cmd, stdin: stdin}, nil
}

// float32BytesLE encodes a float32 slice as little-endian bytes. Currently
// only used by tests but lives here to keep the encoding convention colocated
// with playAudio.
func float32BytesLE(samples []float32) []byte {
	out := make([]byte, len(samples)*4)
	for i, s := range samples {
		binary.LittleEndian.PutUint32(out[i*4:i*4+4], math.Float32bits(s))
	}
	return out
}

// realMain is the testable entry point: parse env, start ffmpeg, wire up
// redis, and pump. Returns an error instead of panicking so tests can drive
// it end-to-end.
func realMain(ctx context.Context) error {
	cfg := &config{}
	if err := env.Parse(cfg); err != nil {
		return fmt.Errorf("failed to parse env config: %w", err)
	}

	sink, err := newFFmpegSink(cfg)
	if err != nil {
		return fmt.Errorf("failed to start ffmpeg: %w", err)
	}
	defer func() {
		if err := sink.Close(); err != nil {
			log.Err(err).Msg("Failed to close ffmpeg sink")
		}
	}()

	r := redispkg.NewClient(&redispkg.Options{
		Addr: fmt.Sprintf("%s:%d", cfg.RedisAddress, cfg.RedisPort),
	})
	defer func() { _ = r.Close() }()

	if err := runLoopFn(ctx, cfg, r, sink); err != nil {
		return fmt.Errorf("icecast_audio_pusher exited with error: %w", err)
	}
	return nil
}

// panicOnErr is the exit hook main() uses when realMain returns an error.
// It's a var so tests can swap it for something observable instead of a
// process-terminating log.Panic.
var panicOnErr = func(err error) {
	log.Panic().Err(err).Msg("icecast_audio_pusher failed")
}

func main() {
	if err := realMain(context.Background()); err != nil {
		panicOnErr(err)
	}
}
