package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

// config holds application configuration from environment variables
type config struct {
	/// Redis
	// Connection info
	RedisAddress string `env:"REDIS_ADDRESS" envDefault:"redis"`
	RedisPort    int    `env:"REDIS_PORT" envDefault:"6379"`
	// Keys
	RedisStatusOutputName string `env:"REDIS_STATUS_OUTPUT_NAME" envDefault:"statuses:openai_text_generator"`
	RedisTriggerKeyName   string `env:"REDIS_TRIGGER_KEY_NAME" envDefault:"session:keepalive"`
	WebpageKeepaliveKey   string `env:"WEBPAGE_KEEPALIVE_KEY" envDefault:"session:keepalive"`
	SessionIDKey          string `env:"SESSION_ID_KEY" envDefault:"session:id"`
	SessionInfoKey        string `env:"SESSION_INFO_KEY" envDefault:"session:info"`
	QwenTTSSpeakerKey     string `env:"QWEN_TTS_SPEAKER_STATUS_KEY" envDefault:"statuses:qwen_tts_speaker"`
	OutputQueueNamePrefix string `env:"REDIS_OUTPUT_QUEUE_NAME_PREFIX" envDefault:"queues:generated_text"`
	TTSVoiceOptionsKey    string `env:"TTS_VOICE_OPTIONS_KEY" envDefault:"options:tts_voices`
	PromptOptionsKey      string `env:"PROMPT_OPTIONS_KEY" envDefault:"options:prompts`

	KeepaliveTTL int `env:"KEEPALIVE_TTL_SECONDS" envDefault:"5"`
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

// Server holds server state
type Server struct {
	mu            sync.RWMutex
	redisClient   *redis.Client
	lastKeepalive time.Time
	keepaliveKey  string
	ticker        *time.Ticker
	canceled      chan struct{}
	cfg           *config
}

// NewServer creates a new server instance
func NewServer(cfg *config) (*Server, error) {
	s := &Server{
		redisClient: redis.NewClient(&redis.Options{
			Addr:     fmt.Sprintf("%s:%d", cfg.RedisAddress, cfg.RedisPort),
			Password: "",
			DB:       0,
		}),
		keepaliveKey:  cfg.WebpageKeepaliveKey,
		lastKeepalive: time.Unix(0, 0),
		ticker:        time.NewTicker(time.Second),
		canceled:      make(chan struct{}),
		cfg:           cfg,
	}

	return s, nil
}

// startKeepaliveMonitor starts the keepalive monitor thread
func (s *Server) startKeepaliveMonitor(ctx context.Context, ttl int) {
	go func() {
		ticker := s.ticker

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.mu.RLock()
				lastTime := s.lastKeepalive
				s.mu.RUnlock()

				now := time.Now()
				elapsed := now.Sub(lastTime)

				// Check if we need to send keepalive (more than TTL seconds since last)
				if elapsed >= time.Duration(ttl)*time.Second {
					s.mu.Lock()
					s.lastKeepalive = time.Now()
					s.mu.Unlock()

					s.redisClient.Set(ctx, s.keepaliveKey, "true", time.Duration(ttl)*time.Second)
				}
			}
		}
	}()
}

// handleWebpageKeepalive handles POST /webpage-keepalive
func (s *Server) handleWebpageKeepalive(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	_, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// TODO: This lol

	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

// handleWebpageStatus handles GET /webpage_status.json
func (s *Server) handleWebpageStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// TODO: convert this to just "Get + handle not found error explicitly"
	// Get OpenAI text generator status
	var openaiStatus OpenAITextGeneratorStatus
	openaiExists, err := s.redisClient.Exists(ctx, s.cfg.RedisStatusOutputName).Result()
	if err != nil {
		log.Err(err).Msg("Failed to check OpenAI status key")
		openaiStatus.Status = "error"
		openaiStatus.AsOf = time.Now().Unix()
	} else if openaiExists > 0 {
		statusData, err := s.redisClient.Get(ctx, s.cfg.RedisStatusOutputName).Bytes()
		if err != nil {
			openaiStatus.Status = "error"
			openaiStatus.AsOf = time.Now().Unix()
		} else {
			err = json.Unmarshal(statusData, &openaiStatus)
			if err != nil {
				log.Err(err).Msg("Failed to parse OpenAI status")
				openaiStatus.Status = "error"
				openaiStatus.AsOf = time.Now().Unix()
			}
		}
	} else {
		openaiStatus.Status = "unknown"
		openaiStatus.AsOf = time.Now().Unix()
	}

	// Get Qwen TTS speaker status
	// lmao what
	// TODO: this
	var qwenStatus QwenTTSSpeakerStatus
	qwenStatus.Status = "unknown"
	qwenStatus.AsOf = time.Now().Unix()

	statusResp := StatusResponse{
		OpenAITextGeneratorStatus: openaiStatus,
		QwenTTSSpeakerStatus:      qwenStatus,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(statusResp)
}

// handleUpdateSession handles POST /update_session
func (s *Server) handleUpdateSession(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse query parameters
	voice := r.URL.Query().Get("voice")
	prompt := r.URL.Query().Get("prompt")

	if voice == "" || prompt == "" {
		log.Warn().Msgf("Missing voice or prompt ID. voice=%q, prompt=%q", voice, prompt)
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Missing voice or prompt parameter"))
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Update session info with voice and prompt IDs
	now := time.Now().Unix()
	sessionInfo := SessionInfo{
		AsOf:     now,
		VoiceID:  voice,
		PromptID: prompt,
	}

	statusData, err := json.Marshal(sessionInfo)
	if err != nil {
		log.Err(err).Msg("Failed to marshal session info")
		http.Error(w, "Marshal error", http.StatusInternalServerError)
		return
	}

	// Update session ID (prompt_id is stored as the session ID)
	err = s.redisClient.Set(ctx, s.cfg.SessionIDKey, prompt, 0).Err()
	if err != nil {
		log.Err(err).Msg("Failed to update session ID in Redis")
		http.Error(w, "Redis error", http.StatusInternalServerError)
		return
	}

	err = s.redisClient.Set(ctx, s.cfg.SessionInfoKey, statusData, 0).Err()
	if err != nil {
		log.Err(err).Msg("Failed to update session info in Redis")
		http.Error(w, "Redis error", http.StatusInternalServerError)
		return
	}

	log.Info().Msgf("Updated session: voice=%q, prompt=%q", voice, prompt)

	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

// handleSpeechOptions handles GET /speech_options
func (s *Server) handleSpeechOptions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Fetch voice options from Redis
	voicesData, voiceOptionsErr := s.redisClient.Get(ctx, s.cfg.TTSVoiceOptionsKey).Bytes()
	var voices []string
	if voiceOptionsErr == nil {
		err := json.Unmarshal(voicesData, &voices)
		if err != nil {
			log.Err(err).Msg("Failed to parse voices from Redis")
			voices = []string{}
		}
	} else {
		log.Err(voiceOptionsErr).Msg("Failed to get voices from Redis")
		voices = []string{}
	}

	// Fetch prompts from Redis
	promptsData, promptOptionsErr := s.redisClient.Get(ctx, s.cfg.PromptOptionsKey).Bytes()
	var prompts []string
	if promptOptionsErr == nil {
		err := json.Unmarshal(promptsData, &prompts)
		if err != nil {
			log.Err(err).Msg("Failed to parse prompts from Redis")
			prompts = []string{}
		}
	} else {
		log.Err(promptOptionsErr).Msg("Failed to get voices from Redis")
		voices = []string{}
	}

	// If everything failed, then just crash out lol
	if voiceOptionsErr != nil && promptOptionsErr != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "Failed to fetch any options data. Voice options err: %s, Prompt options err: %s",
			voiceOptionsErr.Error(), promptOptionsErr.Error())
		return
	}

	options := SpeechOptions{
		Voices:  voices,
		Prompts: prompts,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(options)
}

// handleHealth handles GET /health
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"status": "healthy",
	})
}

func main() {
	cfg := config{}
	err := env.Parse(&cfg)
	if err != nil {
		log.Panic().Msgf("Failed to parse env config: %+v", err)
	}

	server, err := NewServer(&cfg)
	if err != nil {
		log.Panic().Err(err).Msg("Failed to create server")
	}

	// Start keepalive monitor
	ctx := context.Background()
	server.startKeepaliveMonitor(ctx, cfg.KeepaliveTTL)

	// Set up HTTP routes
	http.HandleFunc("/webpage-keepalive", server.handleWebpageKeepalive)
	http.HandleFunc("/webpage_status.json", server.handleWebpageStatus)
	http.HandleFunc("/update_session", server.handleUpdateSession)
	http.HandleFunc("/speech_options", server.handleSpeechOptions)
	http.HandleFunc("/health", server.handleHealth)

	addr := "0.0.0.0:8888"
	log.Info().Msgf("Starting HTTP server on %s", addr)
	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Panic().Err(err).Msg("Failed to start HTTP server")
	}
}
