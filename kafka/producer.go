package kafka

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"time"

	"streamly/media-service/models"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	writer *kafka.Writer
}

func NewProducer(brokers string) *Producer {
	brokerList := strings.Split(brokers, ",")
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokerList...),
		Topic:        "media.uploaded",
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 10 * time.Millisecond,
		Async:        true,
	}
	log.Printf("Kafka producer initialized for topic 'media.uploaded' at brokers: %s", brokers)
	return &Producer{writer: writer}
}

type MediaUploadedEvent struct {
	ID           string    `json:"id"`
	Title        string    `json:"title"`
	Description  string    `json:"description"`
	MediaType    string    `json:"media_type"`
	Artist       string    `json:"artist"`
	Duration     float64   `json:"duration"`
	StreamURL    string    `json:"stream_url"`
	ThumbnailURL string    `json:"thumbnail_url"`
	UploaderID   string    `json:"uploader_id"`
	UploaderName string    `json:"uploader_name"`
	Tags         string    `json:"tags"`
	CreatedAt    time.Time `json:"created_at"`
}

func (p *Producer) PublishMediaUploaded(ctx context.Context, m *models.Media) error {
	if p.writer == nil {
		return nil
	}

	event := MediaUploadedEvent{
		ID:           m.ID,
		Title:        m.Title,
		Description:  m.Description,
		MediaType:    string(m.MediaType),
		Artist:       m.Artist,
		Duration:     m.Duration,
		StreamURL:    m.StreamURL,
		ThumbnailURL: m.ThumbnailURL,
		UploaderID:   m.UploaderID,
		UploaderName: m.UploaderName,
		Tags:         m.Tags,
		CreatedAt:    m.CreatedAt,
	}

	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}

	msg := kafka.Message{
		Key:   []byte(m.ID),
		Value: payload,
		Time:  time.Now(),
	}

	err = p.writer.WriteMessages(ctx, msg)
	if err != nil {
		log.Printf("Failed to publish media.uploaded event to Kafka: %v", err)
		return err
	}

	log.Printf("Successfully published event to Kafka [media.uploaded] for media %s", m.ID)
	return nil
}

func (p *Producer) Close() error {
	if p.writer != nil {
		return p.writer.Close()
	}
	return nil
}
