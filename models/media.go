package models

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

type MediaType string

const (
	TypeVideo MediaType = "video"
	TypeShort MediaType = "short"
	TypeMusic MediaType = "music"
)

type Media struct {
	ID            string    `gorm:"type:uuid;primaryKey" json:"id"`
	Title         string    `gorm:"type:varchar(255);not null;index" json:"title"`
	Description   string    `gorm:"type:text" json:"description"`
	MediaType     MediaType `gorm:"type:varchar(20);not null;index" json:"media_type"`
	Artist        string    `gorm:"type:varchar(100);index" json:"artist,omitempty"`
	Duration      float64   `gorm:"default:0" json:"duration"`
	FilePath      string    `gorm:"type:text;not null" json:"-"`
	StreamURL     string    `gorm:"type:text" json:"stream_url"`
	ThumbnailPath string    `gorm:"type:text" json:"-"`
	ThumbnailURL  string    `gorm:"type:text" json:"thumbnail_url"`
	MimeType      string    `gorm:"type:varchar(100)" json:"mime_type"`
	FileSize      int64     `json:"file_size"`
	UploaderID    string    `gorm:"type:uuid;index" json:"uploader_id"`
	UploaderName  string    `gorm:"type:varchar(100)" json:"uploader_name"`
	Tags          string    `gorm:"type:text" json:"tags"`
	Status        string    `gorm:"type:varchar(30);default:'ready'" json:"status"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

func (m *Media) BeforeCreate(tx *gorm.DB) (err error) {
	if m.ID == "" {
		m.ID = uuid.New().String()
	}
	return
}
