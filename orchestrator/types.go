package main

import (
	"context"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// config holds application configuration from environment variables
type config struct {
	//// Redis
	/// Connection info
	RedisAddress string `env:"REDIS_ADDRESS" envDefault:"redis"`
	RedisPort    int    `env:"REDIS_PORT" envDefault:"6379"`
	/// Keys
	// Component statuses
	RedisStatusOutputName string `env:"REDIS_KEY_STATUSES_OPENAI_TEXT_GENERATOR" envDefault:"statuses:openai_text_generator"`
	QwenTTSSpeakerKey     string `env:"REDIS_KEY_STATUSES_QWEN_TTS_SPEAKER" envDefault:"statuses:qwen_tts_speaker"`
	// Component options
	TTSVoiceOptionsKey string `env:"REDIS_KEY_OPTIONS_TTS_VOICES" envDefault:"options:tts_voices"`
	PromptOptionsKey   string `env:"REDIS_KEY_OPTIONS_PROMPTS" envDefault:"options:prompts"`
	// Session management
	LastHeartbeatKey    string        `env:"REDIS_KEY_SESSION_LAST_HEARTBEAT" envDefault:"session:last_heartbeat"`
	WebpageKeepaliveKey string        `env:"REDIS_KEY_SESSION_WEBPAGE_CONNECTED" envDefault:"session:webpage_connected"`
	SessionIDKey        string        `env:"REDIS_KEY_SESSION_ID" envDefault:"session:id"`
	SessionInfoKey      string        `env:"REDIS_KEY_SESSION_INFO" envDefault:"session:info"`
	KeepaliveTTL        time.Duration `env:"SESSION_KEEPALIVE_TTL_SECONDS" envDefault:"5s"`
}

// OpenAITextGeneratorStatus represents the status of the OpenAI text generator
type OpenAITextGeneratorStatus struct {
	Status string `json:"status"`
	AsOf   int64  `json:"as_of"`
}

// QwenTTSSpeakerStatus represents the status of the Qwen TTS speaker
type QwenTTSSpeakerStatus struct {
	Status string `json:"status"`
	AsOf   int64  `json:"as_of"`
}

// SessionInfo holds information about the current session
type SessionInfo struct {
	AsOf     int64  `json:"as_of"`
	VoiceID  string `json:"voice_id"`
	PromptID string `json:"prompt_id"`
}

// SpeechOptions holds voice and prompt options
type SpeechOptions struct {
	Voices  []string `json:"voices,omitempty"`
	Prompts []string `json:"prompts,omitempty"`
}

// StatusResponse holds the combined status response
type StatusResponse struct {
	OpenAITextGeneratorStatus OpenAITextGeneratorStatus `json:"openai_text_generator_status"`
	QwenTTSSpeakerStatus      QwenTTSSpeakerStatus      `json:"qwen_tts_speaker_status"`
}

// redisClient is the subset of *redis.Client the orchestrator uses, kept as
// an interface so tests can swap in a mock. *redis.Client satisfies this
// implicitly.
type redisClient interface {
	Get(ctx context.Context, key string) *redis.StringCmd
	Set(ctx context.Context, key string, value any, expiration time.Duration) *redis.StatusCmd
	Close() error
}

// Server holds server state
type Server struct {
	mu                   sync.RWMutex
	wg                   sync.WaitGroup
	redisClient          redisClient
	keepaliveKey         string
	keepaliveCheckTicker *time.Ticker
	canceled             chan struct{}
	cfg                  *config
}
