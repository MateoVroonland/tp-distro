FROM golang:latest AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o /main ./cmd/reducers/sentiment_reducer/sentiment_reducer.go

FROM scratch

COPY --from=builder /main /main
COPY .env /app/.env

ENTRYPOINT ["/main"]