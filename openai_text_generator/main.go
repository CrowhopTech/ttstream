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
	redispkg "github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

const PROMPTS_RELDIR = "prompts"

// redisClient is the subset of *redis.Client the openai text generator uses,
// kept as an interface so tests can swap in a mock. *redis.Client satisfies
// this implicitly.
type redisClient interface {
	Get(ctx context.Context, key string) *redispkg.StringCmd
	Set(ctx context.Context, key string, value any, expiration time.Duration) *redispkg.StatusCmd
	Exists(ctx context.Context, keys ...string) *redispkg.IntCmd
	Del(ctx context.Context, keys ...string) *redispkg.IntCmd
	LLen(ctx context.Context, key string) *redispkg.IntCmd
	LPush(ctx context.Context, key string, values ...any) *redispkg.IntCmd
}

type config struct {
	Model                 string `env:"LLAMA_MODEL"`
	LlamaServer           string `env:"LLAMA_SERVER" envDefault:"localhost"`
	LlamaPort             int    `env:"LLAMA_PORT" envDefault:"8080"`
	RedisAddress          string `env:"REDIS_ADDRESS" envDefault:"localhost"`
	RedisPort             int    `env:"REDIS_PORT" envDefault:"6379"`
	RedisStatusOutputName string `env:"REDIS_KEY_STATUSES_OPENAI_TEXT_GENERATOR" envDefault:"statuses:openai_text_generator"`
	RedisTriggerKeyName   string `env:"REDIS_KEY_SESSION_WEBPAGE_CONNECTED" envDefault:"session:webpage_connected"`
	RedisPromptsKeyName   string `env:"REDIS_KEY_OPTIONS_PROMPTS" envDefault:"options:prompts"`
	SessionIDKey          string `env:"REDIS_KEY_SESSION_ID" envDefault:"session:id"`
	SessionInfoKey        string `env:"REDIS_KEY_SESSION_INFO" envDefault:"session:info"`
	OutputQueueNamePrefix string `env:"REDIS_KEY_QUEUES_GENERATED_TEXT" envDefault:"queues:generated_text"`
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

func setStatus(ctx context.Context, redis redisClient, cfg *config, newStatus string) {
	log.Info().Msg(newStatus)
	serialized, err := json.Marshal(OpenAITextGeneratorStatus{status: newStatus, asOf: time.Now().Unix()})
	if err != nil {
		panic(fmt.Sprintf("Failed to marshal JSON in setStatus: %+v", err))
	}
	err = redis.Set(ctx, cfg.RedisStatusOutputName, serialized, 0).Err()
	if err != nil {
		panic(fmt.Sprintf("Failed to set status in Redis: %+v", err))
	}
}

// getPromptsDir returns the directory containing prompt .txt files.
// It's a package-level var so tests can swap it to point at a temp dir.
var getPromptsDir = func() string {
	ex, err := os.Executable()
	if err != nil {
		panic(fmt.Sprintf("Failed to get Executable path: %s", err.Error()))
	}
	log.Debug().Msgf("Executable path is %s, basepath is %s", ex, path.Dir(ex))
	return path.Join(path.Dir(ex), PROMPTS_RELDIR)
}

func loadPromptsToRedis(ctx context.Context, redis redisClient, cfg *config) ([]string, error) {
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
		redis.Set(ctx, cfg.RedisPromptsKeyName, exported, 0)
	} else {
		redis.Set(ctx, cfg.RedisPromptsKeyName, "[]", 0)
	}
	return promptFiles, nil

}

func waitForSessionInit(ctx context.Context, redis redisClient, cfg *config) error {
	log.Info().Msg("Waiting for session to be initialized...")
	timer := time.NewTimer(time.Second)
	for true {
		existsInt, err := redis.Exists(ctx, cfg.SessionIDKey).Result()
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

func getSessionIDAndInfo(ctx context.Context, redis redisClient, cfg *config) (string, SessionInfo, error) {
	sessionID, err := redis.Get(ctx, cfg.SessionIDKey).Result()
	if err != nil {
		return "", SessionInfo{}, fmt.Errorf("failed to get session ID from key '%s': %w", cfg.SessionIDKey, err)
	}
	if strings.TrimSpace(sessionID) == "" {
		return "", SessionInfo{}, fmt.Errorf("session ID at key '%s' is empty", cfg.SessionIDKey)
	}

	infoRaw, err := redis.Get(ctx, cfg.SessionInfoKey).Result()
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

func constructClients() (*config, *openai.Client, *redispkg.Client, *PunctuationChunker, error) {
	var cfg config
	err := env.Parse(cfg)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("failed to parse env config: %+v", err)
	}

	openAIClient := openai.NewClient(
		option.WithBaseURL(fmt.Sprintf("http://%s:%d/v1", cfg.LlamaServer, cfg.LlamaPort)),
		option.WithAPIKey(""),
	)

	return &cfg, &openAIClient, redispkg.NewClient(&redispkg.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.RedisAddress, cfg.RedisPort),
		Password: "",
		DB:       0, // Default DB
	}), NewPunctuationChunker(100), nil
}

func initialSetup(ctx context.Context, cfg *config, redis redisClient) error {
	err := redis.Del(ctx, cfg.RedisPromptsKeyName).Err()
	if err != nil {
		log.Panic().Err(err).Msgf("Failed to wipe Redis key '%s'", cfg.RedisPromptsKeyName)
	}

	return nil
}

func main() {
	ctx := context.Background()

	cfg, openAIClient, redis, chunker, err := constructClients()
	if err != nil {
		log.Panic().Err(err).Msg("Failed to initialize clients")
		return
	}

	setStatus(ctx, redis, cfg, "Starting up...")

	_, err = loadPromptsToRedis(ctx, redis, cfg)
	if err != nil {
		log.Panic().Err(err).Msg("Failed to load prompts to Redis")
	}

	err = waitForSessionInit(ctx, redis, cfg)
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

		latestSessionID, latestSessionInfo, err := getSessionIDAndInfo(ctx, redis, cfg)
		if err != nil {
			log.Err(err).Msg("Failed to get latest session info from Redis")
			continue
		}

		if currentSessionID != latestSessionID {
			log.Info().Msgf("Session ID changed from '%s' to '%s': clearing outdated queue '%s' and reloading", currentSessionID, currentQueueName, latestSessionID)
			if err := redis.Del(ctx, currentQueueName).Err(); err != nil {
				log.Err(err).Msgf("Failed to delete queue '%s' while changing sessions", currentQueueName)
				continue
			}
			currentSessionID, currentSessionInfo = latestSessionID, latestSessionInfo
			currentQueueName = getOutputQueueName(cfg, currentSessionID)
			currentPromptText, err = loadPromptText(currentSessionInfo.PromptID)
			if err != nil {
				log.Err(err).Msgf("Failed to load text for prompt ID '%s'", currentSessionInfo.PromptID)
				continue
			}
			lastAgentMessage = ""
			log.Info().Msgf("Now outputting to queue '%s'", currentQueueName)
		}

		currentQueueLen, err := redis.LLen(ctx, currentQueueName).Result()
		if err != nil {
			log.Err(err).Msgf("Failed to check length of current queue '%s'", currentQueueName)
			continue
		}
		if currentQueueLen >= int64(cfg.MaxQueueLength) {
			queueLenSetStatus.Do(func() {
				setStatus(ctx, redis, cfg, fmt.Sprintf("Waiting for queue to be below length %d (currently %d)", cfg.MaxQueueLength, currentQueueLen))
			})
			continue
		}
		queueLenSetStatus.Reset()

		triggerVal, err := redis.Get(ctx, cfg.RedisTriggerKeyName).Result()
		if err != nil && err != redispkg.Nil {
			log.Err(err).Msgf("Failed to check trigger value at Redis key '%s'", cfg.RedisTriggerKeyName)
			continue
		}
		if err == redispkg.Nil || triggerVal == "" || triggerVal == "false" {
			triggerSetStatus.Do(func() {
				setStatus(ctx, redis, cfg, fmt.Sprintf("Waiting for trigger key '%s' to be set to true...", cfg.RedisTriggerKeyName))

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

		setStatus(ctx, redis, cfg, "Waiting for the model to spit out some text...")
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
			if err = redis.LPush(ctx, currentQueueName, chunk).Err(); err != nil {
				log.Err(err).Msgf("Failed to push last chat chunk to Redis queue '%s'", currentQueueName)
				continue
			}
		}
	}
}
