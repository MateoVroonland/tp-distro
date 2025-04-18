FROM golang:latest AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download


COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o /main ./cmd/reducers/amount_reducer/amount_reducer.go

ENTRYPOINT ["/main"]

FROM scratch

COPY --from=builder /main /main

ENTRYPOINT ["/main"]