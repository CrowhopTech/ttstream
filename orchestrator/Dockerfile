FROM golang:1.26.4
WORKDIR /app

COPY go.mod /app/go.mod
COPY go.sum /app/go.sum
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/orchestrator
RUN chmod +x /app/orchestrator

CMD ["/app/orchestrator"]