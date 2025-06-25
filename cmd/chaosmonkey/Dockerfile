FROM golang:latest AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o /main ./cmd/chaosmonkey/chaos_monkey.go

ENTRYPOINT ["/main"]

FROM alpine:latest

RUN apk add --no-cache docker-cli docker-compose

COPY --from=builder /main /main
COPY .env /app/.env

WORKDIR /app

ENTRYPOINT ["/main"]