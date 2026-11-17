# Build stage
FROM golang:1.23-alpine AS builder

WORKDIR /app

RUN apk add --no-cache git

COPY go.mod go.sum* ./
RUN go mod download || true

COPY . .
RUN go mod tidy
RUN CGO_ENABLED=0 GOOS=linux go build -o media-service .

# Final stage
FROM alpine:3.20

WORKDIR /app

RUN apk --no-cache add ca-certificates tzdata mailcap

COPY --from=builder /app/media-service .

# Create uploads storage directories
RUN mkdir -p /data/uploads/videos /data/uploads/shorts /data/uploads/music /data/uploads/thumbnails

EXPOSE 8081 50051

CMD ["./media-service"]
