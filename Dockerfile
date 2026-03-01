FROM golang:1.26-alpine AS builder

WORKDIR /app

# sqlite3 requires CGO; goolm is pure-Go so no olm-dev needed
RUN apk add --no-cache gcc musl-dev

# Install dependencies
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN MAUTRIX_VERSION=$(grep 'maunium.net/go/mautrix ' go.mod | awk '{print $2}') && \
    go build -tags goolm \
        -ldflags="-s -w -X 'maunium.net/go/mautrix.GoModVersion=$MAUTRIX_VERSION'" \
        -o mautrix-mattermost ./cmd/mautrix-mattermost

# Final stage
FROM alpine:latest

# sqlite is the only runtime dependency when using goolm
RUN apk add --no-cache sqlite-libs ca-certificates

WORKDIR /app

COPY --from=builder /app/mautrix-mattermost .

# Mount config.yaml and registration.yaml via volume or bind mount at runtime.
CMD ["./mautrix-mattermost", "-c", "config.yaml", "-r", "registration.yaml"]
