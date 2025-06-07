FROM golang:latest AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

COPY cmd/sentiment/sentiment_worker.go cmd/sentiment/sentiment_worker.go

RUN CGO_ENABLED=0 GOOS=linux go build -o /main ./cmd/sentiment/sentiment_worker.go

ENTRYPOINT ["/main"]

FROM scratch

COPY --from=builder /main /main
COPY .env /app/.env

ENTRYPOINT ["/main"]