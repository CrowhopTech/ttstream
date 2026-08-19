FROM golang:1.26.4 AS build
WORKDIR /app

COPY go.mod /app/go.mod
COPY go.sum /app/go.sum
COPY vendor /app/vendor
COPY *.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -mod=vendor -o /app/icecast_audio_pusher

FROM debian:bookworm-slim
RUN apt-get -y update && apt-get install -y --no-install-recommends ffmpeg && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /app/icecast_audio_pusher /app/icecast_audio_pusher
CMD ["/app/icecast_audio_pusher"]
