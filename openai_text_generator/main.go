package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/matryer/resync"
	"github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/option"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

const PROMPTS_RELDIR = "prompts"

type config struct {
	Model                 string `env:"LLAMA_MODEL"`
	LlamaServer           string `env:"LLAMA_SERVER" envDefault:"localhost"`
	LlamaPort             int    `env:"LLAMA_PORT" envDefault:"8080"`
	RedisAddress          string `env:"REDIS_ADDRESS" envDefault:"localhost"`
	RedisPort             int    `env:"REDIS_PORT" envDefault:"6379"`
	RedisStatusOutputName string `env:"REDIS_STATUS_OUTPUT_NAME" envDefault:"statuses:openai_text_generator"`
	RedisTriggerKeyName   string `env:"REDIS_TRIGGER_KEY_NAME" envDefault:"session:keepalive"`
	RedisPromptsKeyName   string `env:"REDIS_PROMPTS_KEY_NAME" envDefault:"options:prompts"`
	SessionIDKey          string `env:"REDIS_SESSION_ID_KEY" envDefault:"session:id"`
	SessionInfoKey        string `env:"REDIS_SESSION_INFO_KEY" envDefault:"session:info"`
	OutputQueueNamePrefix string `env:"REDIS_OUTPUT_QUEUE_NAME_PREFIX" envDefault:"queues:generated_text"`
	MaxQueueLength        int    `env:"MAX_QUEUE_LENGTH" envDefault:"10"`
}

type OpenAITextGeneratorStatus struct {
	status string
	asOf   int64
}

type SessionInfo struct {
	AsOf     int64  `json:"as_of"`
	VoiceID  string `json:"voice_id"`
	PromptID string `json:"prompt_id"`
}

type PunctuationChunker struct {
	minCharsPerChunk int
}

func NewPunctuationChunker(minCharsPerChunk int) *PunctuationChunker {
	return &PunctuationChunker{
		minCharsPerChunk: minCharsPerChunk,
	}
}

func (p *PunctuationChunker) PrecleanString(preClean string) string {
	return strings.TrimSpace(strings.ReplaceAll(preClean, "...", "\u2026"))
}

func (p *PunctuationChunker) IsDelimiter(c rune) bool {
	return c == '.' || c == '?' || c == '!' || c == '\u2026'
}

func (p *PunctuationChunker) ChunkString(inputText string) []string {
	var buffer []rune
	chunks := []string{}

	cleaned := p.PrecleanString(inputText)

	for _, c := range cleaned {
		buffer = append(buffer, rune(c))

		if p.IsDelimiter(c) && len(buffer) >= p.minCharsPerChunk {
			trimmed := strings.TrimSpace(string(buffer))
			if len(trimmed) == 0 {
				continue
			}
			chunks = append(chunks, string(buffer))
			buffer = nil
		}
	}
	trimmed := strings.TrimSpace(string(buffer))
	if len(trimmed) > 0 {
		chunks = append(chunks, trimmed)
	}
	return chunks
}

func setStatus(ctx context.Context, r *redis.Client, cfg *config, newStatus string) {
	log.Info().Msg(newStatus)
	serialized, err := json.Marshal(OpenAITextGeneratorStatus{status: newStatus, asOf: time.Now().Unix()})
	if err != nil {
		panic(fmt.Sprintf("Failed to marshal JSON in setStatus: %+v", err))
	}
	err = r.Set(ctx, cfg.RedisStatusOutputName, serialized, 0).Err()
	if err != nil {
		panic(fmt.Sprintf("Failed to set status in Redis: %+v", err))
	}
}

func getPromptsDir() string {
	ex, err := os.Executable()
	if err != nil {
		panic(fmt.Sprintf("Failed to get Executable path: %s", err.Error()))
	}
	log.Debug().Msgf("Executable path is %s, basepath is %s", ex, path.Dir(ex))
	return path.Join(path.Dir(ex), PROMPTS_RELDIR)
}

func loadPromptsToRedis(ctx context.Context, r *redis.Client, cfg *config) ([]string, error) {
	promptsDir := getPromptsDir()
	promptFiles := []string{}

	files, err := os.ReadDir(promptsDir)
	if err != nil {
		return nil, fmt.Errorf("failed to list files in directory '%s': %w", promptsDir, err)
	}
	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".txt") {
			promptFiles = append(promptFiles, strings.TrimSuffix(f.Name(), ".txt"))
		}
	}
	log.Info().Msgf("Loaded %d prompts to Redis key '%s': %+v", len(promptFiles), cfg.RedisPromptsKeyName, strings.Join(promptFiles, ","))
	exported, err := json.Marshal(promptFiles)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal prompts to JSON: %w", err)
	}
	if len(promptFiles) > 0 {
		r.Set(ctx, cfg.RedisPromptsKeyName, exported, 0)
	} else {
		r.Set(ctx, cfg.RedisPromptsKeyName, "[]", 0)
	}
	return promptFiles, nil

}

func waitForSessionInit(ctx context.Context, r *redis.Client, cfg *config) error {
	log.Info().Msg("Waiting for session to be initialized...")
	timer := time.NewTimer(time.Second)
	for true {
		existsInt, err := r.Exists(ctx, cfg.SessionIDKey).Result()
		if err != nil {
			return fmt.Errorf("failed to check if key '%s' exists: %w", cfg.SessionIDKey, err)
		}
		if existsInt > 0 {
			log.Info().Msgf("Session initialized")
			return nil
		}

		select {
		case <-ctx.Done():
			return context.Canceled
		case <-timer.C:
			timer.Reset(time.Second)
			continue
		}
	}
	return fmt.Errorf("unreachable code")
}

func getOutputQueueName(cfg *config, sessionID string) string {
	return fmt.Sprintf("%s:%s", cfg.OutputQueueNamePrefix, sessionID)
}

func getSessionIDAndInfo(ctx context.Context, r *redis.Client, cfg *config) (string, SessionInfo, error) {
	sessionID, err := r.Get(ctx, cfg.SessionIDKey).Result()
	if err != nil {
		return "", SessionInfo{}, fmt.Errorf("failed to get session ID from key '%s': %w", cfg.SessionIDKey, err)
	}
	if strings.TrimSpace(sessionID) == "" {
		return "", SessionInfo{}, fmt.Errorf("session ID at key '%s' is empty", cfg.SessionIDKey)
	}

	infoRaw, err := r.Get(ctx, cfg.SessionInfoKey).Result()
	if err != nil {
		return "", SessionInfo{}, fmt.Errorf("failed to get session info from key '%s': %w", cfg.SessionInfoKey, err)
	}
	infoParsed := SessionInfo{}
	jsonErr := json.Unmarshal([]byte(infoRaw), &infoParsed)
	if jsonErr != nil {
		log.Debug().Msgf("Failed to parse session info '%s': %s", infoRaw, jsonErr.Error())
		return "", SessionInfo{}, fmt.Errorf("failed to parse session info from JSON: %w", jsonErr)
	}

	return sessionID, infoParsed, nil
}

func loadPromptText(promptID string) (string, error) {
	promptFile := path.Join(getPromptsDir(), promptID+".txt")
	contents, err := os.ReadFile(promptFile)
	if err != nil {
		return "", fmt.Errorf("failed to read prompt contents from '%s': %w", promptFile, err)
	}
	return string(contents), nil
}

func main() {
	ctx := context.Background()

	var cfg config
	err := env.Parse(&cfg)
	if err != nil {
		log.Panic().Msgf("Failed to parse env config: %+v", err)
	}

	openAIClient := openai.NewClient(
		option.WithBaseURL(fmt.Sprintf("http://%s:%d/v1", cfg.LlamaServer, cfg.LlamaPort)),
		option.WithAPIKey(""),
	)

	redisClient := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.RedisAddress, cfg.RedisPort),
		Password: "",
		DB:       0, // Default DB
	})

	chunker := NewPunctuationChunker(100)

	setStatus(ctx, redisClient, &cfg, "Starting up...")
	err = redisClient.Del(ctx, cfg.RedisPromptsKeyName).Err()
	if err != nil {
		log.Panic().Err(err).Msgf("Failed to wipe Redis key '%s'", cfg.RedisPromptsKeyName)
	}

	_, err = loadPromptsToRedis(ctx, redisClient, &cfg)
	if err != nil {
		log.Panic().Err(err).Msg("Failed to load prompts to Redis")
	}

	err = waitForSessionInit(ctx, redisClient, &cfg)
	if err != nil {
		log.Panic().Err(err).Msg("Failed to wait for session info to be initialized in Redis")
	}

	var currentSessionID string
	var currentSessionInfo SessionInfo
	var currentQueueName string
	var currentPromptText string
	var lastAgentMessage string

	var queueLenSetStatus resync.Once
	var triggerSetStatus resync.Once

	for true {
		time.Sleep(time.Second)

		latestSessionID, latestSessionInfo, err := getSessionIDAndInfo(ctx, redisClient, &cfg)
		if err != nil {
			log.Err(err).Msg("Failed to get latest session info from Redis")
			continue
		}

		if currentSessionID != latestSessionID {
			log.Info().Msgf("Session ID changed from '%s' to '%s': clearing outdated queue '%s' and reloading", currentSessionID, currentQueueName, latestSessionID)
			if err := redisClient.Del(ctx, currentQueueName).Err(); err != nil {
				log.Err(err).Msgf("Failed to delete queue '%s' while changing sessions", currentQueueName)
				continue
			}
			currentSessionID, currentSessionInfo = latestSessionID, latestSessionInfo
			currentQueueName = getOutputQueueName(&cfg, currentSessionID)
			currentPromptText, err = loadPromptText(currentSessionInfo.PromptID)
			if err != nil {
				log.Err(err).Msgf("Failed to load text for prompt ID '%s'", currentSessionInfo.PromptID)
				continue
			}
			lastAgentMessage = ""
			log.Info().Msgf("Now outputting to queue '%s'", currentQueueName)
		}

		currentQueueLen, err := redisClient.LLen(ctx, currentQueueName).Result()
		if err != nil {
			log.Err(err).Msgf("Failed to check length of current queue '%s'", currentQueueName)
			continue
		}
		if currentQueueLen >= int64(cfg.MaxQueueLength) {
			queueLenSetStatus.Do(func() {
				setStatus(ctx, redisClient, &cfg, fmt.Sprintf("Waiting for queue to be below length %d (currently %d)", cfg.MaxQueueLength, currentQueueLen))
			})
			continue
		}
		queueLenSetStatus.Reset()

		triggerVal, err := redisClient.Get(ctx, cfg.RedisTriggerKeyName).Result()
		if err != nil && err != redis.Nil {
			log.Err(err).Msgf("Failed to check trigger value at Redis key '%s'", cfg.RedisTriggerKeyName)
			continue
		}
		if err == redis.Nil || triggerVal == "" || triggerVal == "false" {
			triggerSetStatus.Do(func() {
				setStatus(ctx, redisClient, &cfg, fmt.Sprintf("Waiting for trigger key '%s' to be set to true...", cfg.RedisTriggerKeyName))

			})
			continue
		}
		triggerSetStatus.Reset()

		chatHistory := []openai.ChatCompletionMessageParamUnion{
			openai.UserMessage(currentPromptText),
		}
		if lastAgentMessage != "" {
			chatHistory = append(chatHistory,
				openai.AssistantMessage(lastAgentMessage),
				openai.UserMessage("Continue."),
			)
		}

		setStatus(ctx, redisClient, &cfg, "Waiting for the model to spit out some text...")
		completion, err := openAIClient.Chat.Completions.New(ctx, openai.ChatCompletionNewParams{
			Messages: chatHistory,
			Model:    cfg.Model,
		})
		if err != nil {
			log.Err(err).Msg("Failed to generate next message in chat")
			continue
		}
		lastAgentMessage = completion.Choices[0].Message.Content
		chunked := chunker.ChunkString(lastAgentMessage)

		for _, chunk := range chunked {
			log.Info().Msgf("Pushing chat chunk (%d chars): '%s'", len(chunk), chunk)
			if err = redisClient.LPush(ctx, currentQueueName, chunk).Err(); err != nil {
				log.Err(err).Msgf("Failed to push last chat chunk to Redis queue '%s'", currentQueueName)
				continue
			}
		}
	}
}
