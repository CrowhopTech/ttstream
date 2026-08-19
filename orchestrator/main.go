package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

// jsonMarshal is used for marshalling values that will be written to Redis.
// It's a package-level var so tests can swap it to force a marshal failure,
// which is otherwise unreachable given the current struct shapes.
var jsonMarshal = json.Marshal

// NewServer creates a new server instance
func NewServer(cfg *config) (*Server, error) {
	s := &Server{
		redisClient: redis.NewClient(&redis.Options{
			Addr:     fmt.Sprintf("%s:%d", cfg.RedisAddress, cfg.RedisPort),
			Password: "",
			DB:       0,
		}),
		keepaliveKey:         cfg.WebpageKeepaliveKey,
		keepaliveCheckTicker: time.NewTicker(time.Second),
		canceled:             make(chan struct{}),
		cfg:                  cfg,
	}

	return s, nil
}

// startKeepaliveMonitor starts the keepalive monitor thread. It exits when ctx is canceled.
func (s *Server) startKeepaliveMonitor(ctx context.Context, ttl time.Duration) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer s.keepaliveCheckTicker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-s.keepaliveCheckTicker.C:
				s.checkKeepalive(ctx, ttl)
			}
		}
	}()
}

// checkKeepalive marks the webpage as disconnected if the last heartbeat is older than ttl.
func (s *Server) checkKeepalive(ctx context.Context, ttl time.Duration) {
	opCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	lastHeartbeat, err := s.redisClient.Get(opCtx, s.cfg.LastHeartbeatKey).Int64()
	switch {
	case errors.Is(err, redis.Nil):
		return
	case errors.Is(err, context.Canceled):
		return
	case err != nil:
		log.Err(err).Msg("Failed to read last heartbeat from Redis")
		return
	}

	if time.Since(time.Unix(lastHeartbeat, 0)) <= ttl {
		return
	}

	if err := s.redisClient.Set(opCtx, s.keepaliveKey, false, 0).Err(); err != nil && !errors.Is(err, context.Canceled) {
		log.Err(err).Msg("Failed to mark webpage keepalive as false in Redis")
	}
}

// handleWebpageHeartbeat handles POST /webpage-keepalive
func (s *Server) handleWebpageHeartbeat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	err := s.redisClient.Set(ctx, s.cfg.LastHeartbeatKey, time.Now().Unix(), 0).Err()
	if err != nil {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "failed to write heartbeat timestamp to redis: %v", err)
		return
	}

	err = s.redisClient.Set(ctx, s.cfg.WebpageKeepaliveKey, true, 0).Err()
	if err != nil {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "failed to write webpage keepalive to redis: %v", err)
		return
	}

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

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	// Get OpenAI text generator status
	var openaiStatus OpenAITextGeneratorStatus
	statusData, err := s.redisClient.Get(ctx, s.cfg.RedisStatusOutputName).Bytes()
	switch {
	case err == redis.Nil:
		openaiStatus.Status = "unknown"
		openaiStatus.AsOf = time.Now().Unix()
	case err != nil:
		log.Err(err).Msg("Failed to get OpenAI status key")
		openaiStatus.Status = "error"
		openaiStatus.AsOf = time.Now().Unix()
	default:
		if err := json.Unmarshal(statusData, &openaiStatus); err != nil {
			log.Err(err).Msg("Failed to parse OpenAI status")
			openaiStatus.Status = "error"
			openaiStatus.AsOf = time.Now().Unix()
		}
	}

	// Get Qwen TTS speaker status
	var qwenStatus QwenTTSSpeakerStatus
	qwenData, err := s.redisClient.Get(ctx, s.cfg.QwenTTSSpeakerKey).Bytes()
	switch {
	case err == redis.Nil:
		qwenStatus.Status = "unknown"
		qwenStatus.AsOf = time.Now().Unix()
	case err != nil:
		log.Err(err).Msg("Failed to get Qwen TTS speaker status key")
		qwenStatus.Status = "error"
		qwenStatus.AsOf = time.Now().Unix()
	default:
		if err := json.Unmarshal(qwenData, &qwenStatus); err != nil {
			log.Err(err).Msg("Failed to parse Qwen TTS speaker status")
			qwenStatus.Status = "error"
			qwenStatus.AsOf = time.Now().Unix()
		}
	}

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

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	// Update session info with voice and prompt IDs
	now := time.Now().Unix()
	sessionInfo := SessionInfo{
		AsOf:     now,
		VoiceID:  voice,
		PromptID: prompt,
	}

	statusData, err := jsonMarshal(sessionInfo)
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

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
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
	if err := env.Parse(&cfg); err != nil {
		log.Panic().Msgf("Failed to parse env config: %+v", err)
	}

	// Root context — canceled on SIGINT/SIGTERM. Everything downstream (background
	// goroutines, in-flight HTTP handlers, Redis calls) observes this cancellation.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	server, err := NewServer(&cfg)
	if err != nil {
		log.Panic().Err(err).Msg("Failed to create server")
	}

	server.startKeepaliveMonitor(ctx, cfg.KeepaliveTTL)

	mux := http.NewServeMux()
	mux.HandleFunc("/webpage-keepalive", server.handleWebpageHeartbeat)
	mux.HandleFunc("/webpage_status.json", server.handleWebpageStatus)
	mux.HandleFunc("/update_session", server.handleUpdateSession)
	mux.HandleFunc("/speech_options", server.handleSpeechOptions)
	mux.HandleFunc("/health", server.handleHealth)

	httpServer := &http.Server{
		Addr:              "0.0.0.0:8888",
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		// BaseContext ties every request's r.Context() to the root context, so
		// signal cancellation flows through in-flight handlers too.
		BaseContext: func(_ net.Listener) context.Context { return ctx },
	}

	serverErr := make(chan error, 1)
	go func() {
		log.Info().Msgf("Starting HTTP server on %s", httpServer.Addr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
		close(serverErr)
	}()

	select {
	case <-ctx.Done():
		log.Info().Msg("Shutdown signal received, draining connections (send signal again to force exit)")
	case err := <-serverErr:
		if err != nil {
			log.Err(err).Msg("HTTP server failed")
		}
	}

	// Arm the panic button: a second signal during drain exits immediately.
	// stop() from signal.NotifyContext already reset the handler, so re-register.
	forceExit := make(chan os.Signal, 1)
	signal.Notify(forceExit, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-forceExit
		log.Warn().Msg("Second signal received, forcing exit")
		os.Exit(1)
	}()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Err(err).Msg("HTTP server graceful shutdown failed")
	}

	server.wg.Wait()

	if err := server.redisClient.Close(); err != nil {
		log.Err(err).Msg("Failed to close Redis client")
	}

	log.Info().Msg("Shutdown complete")
}
