FROM golang:latest AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

COPY cmd/resuscitator/resuscitator.go cmd/resuscitator/resuscitator.go

RUN CGO_ENABLED=0 GOOS=linux go build -o /main ./cmd/resuscitator/resuscitator.go

ENTRYPOINT ["/main"]

FROM alpine:latest

# Install Docker CLI
RUN apk add --no-cache docker-cli docker-compose

COPY --from=builder /main /main
COPY .env /app/.env

WORKDIR /app

ENTRYPOINT ["/main"]