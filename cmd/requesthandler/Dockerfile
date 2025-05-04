FROM golang:latest AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

COPY cmd/requesthandler/request_handler.go cmd/requesthandler/request_handler.go

RUN CGO_ENABLED=0 GOOS=linux go build -o /main ./cmd/requesthandler/request_handler.go

ENTRYPOINT ["/main"]

FROM scratch

COPY --from=builder /main /main
COPY .env /app/.env

ENTRYPOINT ["/main"]