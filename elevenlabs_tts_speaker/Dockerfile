FROM golang:1.26.4
WORKDIR /app

COPY go.mod /app/go.mod
COPY go.sum /app/go.sum
COPY vendor /app/vendor
COPY *.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -mod=vendor -o /app/elevenlabs_tts_speaker
RUN chmod +x /app/elevenlabs_tts_speaker

CMD ["/app/elevenlabs_tts_speaker"]
