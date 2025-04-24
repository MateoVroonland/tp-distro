FROM golang:latest AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download


COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o /main ./cmd/sinks_q3/q3_sinks.go

ENTRYPOINT ["/main"]

FROM scratch

COPY --from=builder /main /main

ENTRYPOINT ["/main"]