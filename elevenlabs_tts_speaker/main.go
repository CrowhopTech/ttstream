package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"time"

	"github.com/caarlos0/env/v11"
	elevenlabs "github.com/plexusone/elevenlabs-go"
	"github.com/plexusone/elevenlabs-go/tts"
	redispkg "github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

// ElevenLabs streams PCM as 16-bit little-endian signed integers. We convert
// each sample to a float32 in [-1, 1] to match the downstream format used by
// the Qwen TTS speaker.
const (
	int16Max     = 32768.0
	pcmOutputFmt = "pcm_24000"
)

// redisClient is the subset of *redis.Client the elevenlabs speaker uses,
// kept as an interface so tests can swap in a mock. *redis.Client satisfies
// this implicitly.
type redisClient interface {
	Get(ctx context.Context, key string) *redispkg.StringCmd
	Set(ctx context.Context, key string, value any, expiration time.Duration) *redispkg.StatusCmd
	Exists(ctx context.Context, keys ...string) *redispkg.IntCmd
	Del(ctx context.Context, keys ...string) *redispkg.IntCmd
	LPush(ctx context.Context, key string, values ...any) *redispkg.IntCmd
	RPop(ctx context.Context, key string) *redispkg.StringCmd
}

// ttsGenerator is the subset of *tts.Service used here, so tests can swap it.
type ttsGenerator interface {
	Generate(ctx context.Context, req *tts.Request) (*tts.Response, error)
}

type config struct {
	APIKey                string `env:"ELEVENLABS_API_KEY,required"`
	Model                 string `env:"ELEVENLABS_MODEL" envDefault:"eleven_turbo_v2_5"`
	RedisAddress          string `env:"REDIS_ADDRESS" envDefault:"localhost"`
	RedisPort             int    `env:"REDIS_PORT" envDefault:"6379"`
	RedisStatusOutputName string `env:"REDIS_KEY_STATUSES_ELEVENLABS_TTS_SPEAKER" envDefault:"statuses:elevenlabs_tts_speaker"`
	RedisTriggerKeyName   string `env:"REDIS_KEY_SESSION_WEBPAGE_CONNECTED" envDefault:"session:webpage_connected"`
	SessionIDKey          string `env:"REDIS_KEY_SESSION_ID" envDefault:"session:id"`
	SessionInfoKey        string `env:"REDIS_KEY_SESSION_INFO" envDefault:"session:info"`
	RedisVoicesKeyName    string `env:"REDIS_KEY_OPTIONS_TTS_VOICES" envDefault:"options:tts_voices"`
	InputQueueBase        string `env:"REDIS_KEY_QUEUES_GENERATED_TEXT" envDefault:"queues:generated_text"`
	OutputQueueBase       string `env:"REDIS_KEY_QUEUES_GENERATED_AUDIO_BYTES" envDefault:"queues:generated_audio_bytes"`
	VoicesFilePath        string `env:"VOICES_FILE" envDefault:"voices.json"`
}

// ElevenLabsTTSSpeakerStatus mirrors the status payload shape written by the
// Python implementation: `{"as_of": <unix seconds>, "status": "..."}`.
type ElevenLabsTTSSpeakerStatus struct {
	AsOf   int64  `json:"as_of"`
	Status string `json:"status"`
}

// SessionInfo mirrors the payload the orchestrator writes to session:info.
type SessionInfo struct {
	AsOf     int64  `json:"as_of"`
	VoiceID  string `json:"voice_id"`
	PromptID string `json:"prompt_id"`
}

// Voice is one entry inside voices.json.
type Voice struct {
	ElevenLabsVoiceID string `json:"elevenlabs_voice_id"`
}

func setStatus(ctx context.Context, r redisClient, cfg *config, newStatus string) {
	log.Info().Msg(newStatus)
	payload, err := json.Marshal(ElevenLabsTTSSpeakerStatus{AsOf: time.Now().Unix(), Status: newStatus})
	if err != nil {
		log.Panic().Err(err).Msg("Failed to marshal status JSON")
	}
	if err := r.Set(ctx, cfg.RedisStatusOutputName, payload, 0).Err(); err != nil {
		log.Panic().Err(err).Msg("Failed to write status to Redis")
	}
}

func loadVoices(path string) (map[string]Voice, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read voices file '%s': %w", path, err)
	}
	var parsed map[string]Voice
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return nil, fmt.Errorf("failed to parse voices file '%s': %w", path, err)
	}
	if len(parsed) == 0 {
		return nil, fmt.Errorf("voices file '%s' contains no entries", path)
	}
	return parsed, nil
}

// publishVoices writes the parsed voices.json back out to Redis so the
// orchestrator's /speech_options endpoint can enumerate them.
func publishVoices(ctx context.Context, r redisClient, cfg *config, voices map[string]Voice) error {
	payload, err := json.Marshal(voices)
	if err != nil {
		return fmt.Errorf("failed to marshal voices: %w", err)
	}
	if err := r.Set(ctx, cfg.RedisVoicesKeyName, payload, 0).Err(); err != nil {
		return fmt.Errorf("failed to write voices to Redis: %w", err)
	}
	return nil
}

func waitForSessionInit(ctx context.Context, r redisClient, cfg *config) error {
	log.Info().Msg("ElevenLabs TTS speaker waiting for session initialization...")
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		exists, err := r.Exists(ctx, cfg.SessionIDKey).Result()
		if err != nil {
			return fmt.Errorf("failed to check if key '%s' exists: %w", cfg.SessionIDKey, err)
		}
		if exists > 0 {
			sid, err := r.Get(ctx, cfg.SessionIDKey).Result()
			if err != nil {
				return fmt.Errorf("failed to read session ID: %w", err)
			}
			log.Info().Msgf("Session initialized: %s", sid)
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// resolveVoiceID looks at session:info to pick the caller-selected voice, and
// otherwise falls back to the first entry in voices.json.
func resolveVoiceID(ctx context.Context, r redisClient, cfg *config, voices map[string]Voice) (string, error) {
	firstFallback := func() (string, error) {
		for name, v := range voices {
			log.Warn().Msgf("No voice_id in session info, using first available voice: %s", name)
			return v.ElevenLabsVoiceID, nil
		}
		return "", errors.New("no voices available in voices.json")
	}

	raw, err := r.Get(ctx, cfg.SessionInfoKey).Result()
	if errors.Is(err, redispkg.Nil) {
		log.Warn().Msg("No session info found")
		return firstFallback()
	}
	if err != nil {
		return "", fmt.Errorf("failed to read session info: %w", err)
	}

	var info SessionInfo
	if err := json.Unmarshal([]byte(raw), &info); err != nil {
		log.Warn().Err(err).Msgf("Failed to parse session info '%s'", raw)
		return firstFallback()
	}
	if info.VoiceID == "" {
		return firstFallback()
	}
	v, ok := voices[info.VoiceID]
	if !ok {
		log.Warn().Msgf("Voice ID '%s' from session info not in voices.json", info.VoiceID)
		return firstFallback()
	}
	return v.ElevenLabsVoiceID, nil
}

// generateSpeech converts text to a float32 PCM byte slice at 24kHz.
func generateSpeech(ctx context.Context, svc ttsGenerator, model, voiceID, text string) ([]byte, error) {
	resp, err := svc.Generate(ctx, &tts.Request{
		VoiceID:      voiceID,
		Text:         text,
		ModelID:      model,
		OutputFormat: pcmOutputFmt,
	})
	if err != nil {
		return nil, fmt.Errorf("elevenlabs generate failed: %w", err)
	}
	pcm, err := io.ReadAll(resp.Audio)
	if err != nil {
		return nil, fmt.Errorf("failed to read pcm stream: %w", err)
	}
	if len(pcm)%2 != 0 {
		return nil, fmt.Errorf("pcm stream length %d not aligned to int16 samples", len(pcm))
	}

	nSamples := len(pcm) / 2
	out := make([]byte, nSamples*4)
	for i := range nSamples {
		s := int16(binary.LittleEndian.Uint16(pcm[i*2 : i*2+2]))
		f := float32(float64(s) / int16Max)
		binary.LittleEndian.PutUint32(out[i*4:i*4+4], math.Float32bits(f))
	}
	return out, nil
}

func inputQueueName(cfg *config, sessionID string) string {
	return fmt.Sprintf("%s:%s", cfg.InputQueueBase, sessionID)
}

func outputQueueName(cfg *config, sessionID string) string {
	return fmt.Sprintf("%s:%s", cfg.OutputQueueBase, sessionID)
}

// waitForTrigger blocks until the trigger key is set to something other than
// "" or "false". Returns the current session ID once cleared to unblock.
func waitForTrigger(ctx context.Context, r redisClient, cfg *config) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		val, err := r.Get(ctx, cfg.RedisTriggerKeyName).Result()
		if err != nil && !errors.Is(err, redispkg.Nil) {
			return fmt.Errorf("failed to read trigger key '%s': %w", cfg.RedisTriggerKeyName, err)
		}
		if !errors.Is(err, redispkg.Nil) && val != "" && val != "false" {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// popNextText polls the input queue for a new text chunk to render. Returns
// the text once one becomes available.
func popNextText(ctx context.Context, r redisClient, queue string) (string, error) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		val, err := r.RPop(ctx, queue).Result()
		if err == nil {
			return val, nil
		}
		if !errors.Is(err, redispkg.Nil) {
			return "", fmt.Errorf("failed to rpop '%s': %w", queue, err)
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-ticker.C:
		}
	}
}

func run(ctx context.Context, cfg *config, r redisClient, svc ttsGenerator) error {
	voices, err := loadVoices(cfg.VoicesFilePath)
	if err != nil {
		return err
	}
	log.Debug().Msgf("Loaded %d voices from '%s'", len(voices), cfg.VoicesFilePath)
	if err := publishVoices(ctx, r, cfg, voices); err != nil {
		return err
	}

	if err := waitForSessionInit(ctx, r, cfg); err != nil {
		return err
	}

	sessionID, err := r.Get(ctx, cfg.SessionIDKey).Result()
	if err != nil {
		return fmt.Errorf("failed to read session ID: %w", err)
	}
	inputQueue := inputQueueName(cfg, sessionID)
	outputQueue := outputQueueName(cfg, sessionID)

	voiceID, err := resolveVoiceID(ctx, r, cfg, voices)
	if err != nil {
		return err
	}

	setStatus(ctx, r, cfg, "ElevenLabs TTS speaker is ready")

	// Warmup: surface auth/config errors immediately.
	if _, err := generateSpeech(ctx, svc, cfg.Model, voiceID, "Warmup"); err != nil {
		return fmt.Errorf("warmup generation failed: %w", err)
	}

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		triggerVal, err := r.Get(ctx, cfg.RedisTriggerKeyName).Result()
		triggerMissing := errors.Is(err, redispkg.Nil)
		if err != nil && !triggerMissing {
			log.Err(err).Msgf("Failed to read trigger key '%s'", cfg.RedisTriggerKeyName)
			time.Sleep(time.Second)
			continue
		}
		if triggerMissing || triggerVal == "" || triggerVal == "false" {
			setStatus(ctx, r, cfg, "Waiting for trigger key to be set to true...")
			log.Warn().Msg("Clearing generated audio queue")
			if err := r.Del(ctx, outputQueue).Err(); err != nil {
				log.Err(err).Msgf("Failed to delete output queue '%s'", outputQueue)
			}
			if err := waitForTrigger(ctx, r, cfg); err != nil {
				return err
			}
			// Re-check session ID and voice after trigger clears.
			latestSessionID, err := r.Get(ctx, cfg.SessionIDKey).Result()
			if err != nil {
				log.Err(err).Msg("Failed to re-read session ID after trigger")
				continue
			}
			if latestSessionID != sessionID {
				sessionID = latestSessionID
				inputQueue = inputQueueName(cfg, sessionID)
				outputQueue = outputQueueName(cfg, sessionID)
				log.Info().Msgf("Session ID changed, now consuming '%s' and pushing to '%s'", inputQueue, outputQueue)
			}
			voiceID, err = resolveVoiceID(ctx, r, cfg, voices)
			if err != nil {
				log.Err(err).Msg("Failed to resolve voice ID after trigger")
				continue
			}
		}

		setStatus(ctx, r, cfg, "Waiting for text to render to audio...")
		nextText, err := popNextText(ctx, r, inputQueue)
		if err != nil {
			return err
		}

		setStatus(ctx, r, cfg, fmt.Sprintf("Generating audio for text '%s'...", nextText))
		audio, err := generateSpeech(ctx, svc, cfg.Model, voiceID, nextText)
		if err != nil {
			log.Err(err).Msgf("Failed to generate audio for text '%s'", nextText)
			continue
		}
		if err := r.LPush(ctx, outputQueue, audio).Err(); err != nil {
			log.Err(err).Msgf("Failed to push audio bytes to queue '%s'", outputQueue)
			continue
		}
		log.Info().Msgf("Successfully generated audio for text '%s'", nextText)
	}
}

func main() {
	cfg := &config{}
	if err := env.Parse(cfg); err != nil {
		log.Panic().Err(err).Msg("Failed to parse env config")
	}

	log.Debug().Msg("Constructing elevenlabs client")
	client, err := elevenlabs.NewClient(elevenlabs.WithAPIKey(cfg.APIKey))
	if err != nil {
		log.Panic().Err(err).Msg("Failed to construct ElevenLabs client")
	}
	log.Debug().Msg("Constructed elevenlabs client")

	r := redispkg.NewClient(&redispkg.Options{
		Addr: fmt.Sprintf("%s:%d", cfg.RedisAddress, cfg.RedisPort),
	})

	if err := run(context.Background(), cfg, r, client.TTS()); err != nil {
		log.Panic().Err(err).Msg("ElevenLabs TTS speaker exited with error")
	}
}
