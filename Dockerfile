# syntax=docker/dockerfile:1.7

FROM golang:1.25-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /bin/order-api ./cmd/order-api

RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /bin/inventory-service ./cmd/inventory-service

RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /bin/payment-service ./cmd/payment-service

RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /bin/shipping-service ./cmd/shipping-service

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /bin/order-api /bin/order-api
COPY --from=builder /bin/inventory-service /bin/inventory-service
COPY --from=builder /bin/payment-service /bin/payment-service
COPY --from=builder /bin/shipping-service /bin/shipping-service
COPY config.docker.yaml /app/config.yaml

USER nonroot:nonroot
