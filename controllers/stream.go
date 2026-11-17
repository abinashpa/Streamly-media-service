package controllers

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"

	"streamly/media-service/config"
	"streamly/media-service/models"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

type StreamController struct {
	DB  *gorm.DB
	Cfg config.Config
}

func NewStreamController(db *gorm.DB, cfg config.Config) *StreamController {
	return &StreamController{DB: db, Cfg: cfg}
}

// StreamMedia serves audio and video with full HTTP 206 Partial Content (Range requests)
func (sc *StreamController) StreamMedia(c *gin.Context) {
	id := c.Param("id")

	var media models.Media
	if err := sc.DB.First(&media, "id = ?", id).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Media item not found"})
		return
	}

	if media.FilePath == "" {
		c.JSON(http.StatusNotFound, gin.H{"error": "Media file not found on disk"})
		return
	}

	filePath := filepath.Clean(media.FilePath)
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			c.JSON(http.StatusNotFound, gin.H{"error": "Media file not found on server"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Unable to access file"})
		return
	}

	// Set essential streaming headers
	c.Header("Accept-Ranges", "bytes")
	if media.MimeType != "" {
		c.Header("Content-Type", media.MimeType)
	}

	// http.ServeFile implements full RFC 7233 Range request support (HTTP 206 Partial Content)
	http.ServeFile(c.Writer, c.Request, filePath)
	_ = fileInfo
}

// ServeThumbnail serves thumbnail images
func (sc *StreamController) ServeThumbnail(c *gin.Context) {
	id := c.Param("id")

	var media models.Media
	if err := sc.DB.First(&media, "id = ?", id).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Media item not found"})
		return
	}

	if media.ThumbnailPath == "" {
		// Return placeholder or 404
		c.Redirect(http.StatusTemporaryRedirect, fmt.Sprintf("https://picsum.photos/seed/%s/640/360", id))
		return
	}

	filePath := filepath.Clean(media.ThumbnailPath)
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		c.Redirect(http.StatusTemporaryRedirect, fmt.Sprintf("https://picsum.photos/seed/%s/640/360", id))
		return
	}

	c.Header("Cache-Control", "public, max-age=86400")
	http.ServeFile(c.Writer, c.Request, filePath)
}
