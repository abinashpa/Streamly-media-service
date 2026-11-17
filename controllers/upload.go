package controllers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"streamly/media-service/config"
	mediagrpc "streamly/media-service/grpc"
	mediakafka "streamly/media-service/kafka"
	"streamly/media-service/models"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

type UploadController struct {
	DB            *gorm.DB
	RDB           *redis.Client
	KafkaProducer *mediakafka.Producer
	Cfg           config.Config
}

func NewUploadController(db *gorm.DB, rdb *redis.Client, kafkaProducer *mediakafka.Producer, cfg config.Config) *UploadController {
	// Ensure upload directories exist
	dirs := []string{
		filepath.Join(cfg.UploadDir, "videos"),
		filepath.Join(cfg.UploadDir, "shorts"),
		filepath.Join(cfg.UploadDir, "music"),
		filepath.Join(cfg.UploadDir, "thumbnails"),
	}
	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			log.Printf("Failed to create dir %s: %v", dir, err)
		}
	}
	return &UploadController{DB: db, RDB: rdb, KafkaProducer: kafkaProducer, Cfg: cfg}
}

// UploadMedia handles uploading video, short video, or music files along with optional thumbnails
func (uc *UploadController) UploadMedia(c *gin.Context) {
	// Multipart form parse (up to 500MB)
	if err := c.Request.ParseMultipartForm(500 << 20); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "File size exceeds maximum allowed limit (500MB)"})
		return
	}

	title := strings.TrimSpace(c.PostForm("title"))
	if title == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Title is required"})
		return
	}

	mediaTypeStr := strings.ToLower(strings.TrimSpace(c.PostForm("media_type")))
	var mediaType models.MediaType
	switch mediaTypeStr {
	case "short", "shorts":
		mediaType = models.TypeShort
	case "music", "audio", "song":
		mediaType = models.TypeMusic
	default:
		mediaType = models.TypeVideo
	}

	description := c.PostForm("description")
	artist := c.PostForm("artist")
	tags := c.PostForm("tags")
	uploaderID := c.PostForm("uploader_id")
	uploaderName := c.PostForm("uploader_name")
	if uploaderName == "" {
		uploaderName = "Streamly Creator"
	}
	durationStr := c.PostForm("duration")
	var duration float64
	if d, err := strconv.ParseFloat(durationStr, 64); err == nil {
		duration = d
	}

	// 1. Process media file
	fileHeader, err := c.FormFile("file")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Media file is required under 'file' key"})
		return
	}

	fileExt := strings.ToLower(filepath.Ext(fileHeader.Filename))
	allowedExts := map[string]bool{
		".mp4": true, ".webm": true, ".mov": true, ".mkv": true,
		".mp3": true, ".wav": true, ".ogg": true, ".flac": true, ".m4a": true,
	}
	if !allowedExts[fileExt] {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("Unsupported file format: %s", fileExt)})
		return
	}

	mediaID := uuid.New().String()
	folderName := "videos"
	if mediaType == models.TypeShort {
		folderName = "shorts"
	} else if mediaType == models.TypeMusic {
		folderName = "music"
	}

	mediaFilename := fmt.Sprintf("%s%s", mediaID, fileExt)
	destPath := filepath.Join(uc.Cfg.UploadDir, folderName, mediaFilename)

	if err := c.SaveUploadedFile(fileHeader, destPath); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to save media file"})
		return
	}

	// Determine MIME type
	mimeType := fileHeader.Header.Get("Content-Type")
	if mimeType == "" || mimeType == "application/octet-stream" {
		switch fileExt {
		case ".mp4":
			mimeType = "video/mp4"
		case ".webm":
			mimeType = "video/webm"
		case ".mp3":
			mimeType = "audio/mpeg"
		case ".wav":
			mimeType = "audio/wav"
		case ".ogg":
			mimeType = "audio/ogg"
		case ".flac":
			mimeType = "audio/flac"
		}
	}

	// 2. Process optional thumbnail
	var thumbDestPath string
	var thumbnailURL string
	thumbHeader, err := c.FormFile("thumbnail")
	if err == nil {
		thumbExt := strings.ToLower(filepath.Ext(thumbHeader.Filename))
		thumbFilename := fmt.Sprintf("%s_thumb%s", mediaID, thumbExt)
		thumbDestPath = filepath.Join(uc.Cfg.UploadDir, "thumbnails", thumbFilename)
		if err := c.SaveUploadedFile(thumbHeader, thumbDestPath); err == nil {
			thumbnailURL = fmt.Sprintf("/api/media/stream/%s/thumbnail", mediaID)
		}
	} else {
		// Provide default thumbnail URL
		if mediaType == models.TypeMusic {
			thumbnailURL = fmt.Sprintf("https://images.unsplash.com/photo-1511671782779-c97d3d27a1d4?w=600&auto=format&fit=crop&q=80")
		} else if mediaType == models.TypeShort {
			thumbnailURL = fmt.Sprintf("https://images.unsplash.com/photo-1534447677768-be436bb09401?w=600&auto=format&fit=crop&q=80")
		} else {
			thumbnailURL = fmt.Sprintf("https://images.unsplash.com/photo-1518770660439-4636190af475?w=600&auto=format&fit=crop&q=80")
		}
	}

	streamURL := fmt.Sprintf("/api/media/stream/%s", mediaID)

	mediaRecord := models.Media{
		ID:            mediaID,
		Title:         title,
		Description:   description,
		MediaType:     mediaType,
		Artist:        artist,
		Duration:      duration,
		FilePath:      destPath,
		StreamURL:     streamURL,
		ThumbnailPath: thumbDestPath,
		ThumbnailURL:  thumbnailURL,
		MimeType:      mimeType,
		FileSize:      fileHeader.Size,
		UploaderID:    uploaderID,
		UploaderName:  uploaderName,
		Tags:          tags,
		Status:        "ready",
	}

	if err := uc.DB.Create(&mediaRecord).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to record media metadata in DB"})
		return
	}

	// 3. Publish event to Apache Kafka queue (media.uploaded topic)
	if uc.KafkaProducer != nil {
		go uc.KafkaProducer.PublishMediaUploaded(context.Background(), &mediaRecord)
	}

	// 4. Asynchronously notify OpenSearch search-service & Redis cache
	go uc.indexInSearchService(&mediaRecord)
	go uc.invalidateCache()

	c.JSON(http.StatusCreated, gin.H{
		"message": "Media uploaded successfully",
		"media":   mediaRecord,
	})
}

func (uc *UploadController) indexInSearchService(m *models.Media) {
	// 1. Primary: East-West inter-service communication via gRPC
	if uc.Cfg.SearchServiceGRPCAddr != "" {
		if err := mediagrpc.IndexMediaInSearchServiceGRPC(uc.Cfg.SearchServiceGRPCAddr, m); err == nil {
			return
		} else {
			log.Printf("gRPC index failed, falling back to REST: %v", err)
		}
	}

	// 2. Fallback: REST HTTP POST
	doc := map[string]interface{}{
		"id":            m.ID,
		"title":         m.Title,
		"description":   m.Description,
		"media_type":    string(m.MediaType),
		"artist":        m.Artist,
		"duration":      m.Duration,
		"stream_url":    m.StreamURL,
		"thumbnail_url": m.ThumbnailURL,
		"uploader_id":   m.UploaderID,
		"uploader_name": m.UploaderName,
		"tags":          m.Tags,
		"created_at":    m.CreatedAt.Format(time.RFC3339),
	}

	body, err := json.Marshal(doc)
	if err != nil {
		log.Printf("Failed to marshal search doc: %v", err)
		return
	}

	url := fmt.Sprintf("%s/api/v1/search/index", uc.Cfg.SearchServiceURL)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(body))
	if err != nil {
		log.Printf("Failed to create search index request: %v", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("Warning: failed to index in search service: %v", err)
		return
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
}

func (uc *UploadController) invalidateCache() {
	if uc.RDB == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Clear cached feed keys
	keys, err := uc.RDB.Keys(ctx, "catalog:*").Result()
	if err == nil && len(keys) > 0 {
		uc.RDB.Del(ctx, keys...)
	}
}
