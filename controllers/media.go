package controllers

import (
	"net/http"
	"os"

	"streamly/media-service/config"
	"streamly/media-service/models"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

type MediaController struct {
	DB  *gorm.DB
	Cfg config.Config
}

func NewMediaController(db *gorm.DB, cfg config.Config) *MediaController {
	return &MediaController{DB: db, Cfg: cfg}
}

func (mc *MediaController) GetMedia(c *gin.Context) {
	id := c.Param("id")

	var media models.Media
	if err := mc.DB.First(&media, "id = ?", id).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Media not found"})
		return
	}

	c.JSON(http.StatusOK, media)
}

func (mc *MediaController) ListMedia(c *gin.Context) {
	mediaType := c.Query("type")
	uploaderID := c.Query("uploader_id")

	query := mc.DB.Order("created_at DESC").Limit(50)

	if mediaType != "" {
		query = query.Where("media_type = ?", mediaType)
	}
	if uploaderID != "" {
		query = query.Where("uploader_id = ?", uploaderID)
	}

	var items []models.Media
	if err := query.Find(&items).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch media list"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"items": items,
		"count": len(items),
	})
}

func (mc *MediaController) DeleteMedia(c *gin.Context) {
	id := c.Param("id")

	var media models.Media
	if err := mc.DB.First(&media, "id = ?", id).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Media not found"})
		return
	}

	// Remove physical files
	if media.FilePath != "" {
		_ = os.Remove(media.FilePath)
	}
	if media.ThumbnailPath != "" {
		_ = os.Remove(media.ThumbnailPath)
	}

	if err := mc.DB.Delete(&media).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to delete media record"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Media deleted successfully"})
}
