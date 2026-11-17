package routes

import (
	"streamly/media-service/config"
	"streamly/media-service/controllers"
	mediakafka "streamly/media-service/kafka"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

func SetupRouter(db *gorm.DB, rdb *redis.Client, kafkaProducer *mediakafka.Producer, cfg config.Config) *gin.Engine {
	r := gin.Default()

	r.Use(cors.New(cors.Config{
		AllowAllOrigins:  true,
		AllowMethods:     []string{"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"},
		AllowHeaders:     []string{"Origin", "Content-Length", "Content-Type", "Authorization", "Range"},
		ExposeHeaders:    []string{"Content-Length", "Content-Range", "Accept-Ranges"},
		AllowCredentials: true,
	}))

	r.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok", "service": "media-service"})
	})

	uploadCtrl := controllers.NewUploadController(db, rdb, kafkaProducer, cfg)
	streamCtrl := controllers.NewStreamController(db, cfg)
	mediaCtrl := controllers.NewMediaController(db, cfg)

	api := r.Group("/api/v1/media")
	{
		api.POST("/upload", uploadCtrl.UploadMedia)
		api.GET("", mediaCtrl.ListMedia)
		api.GET("/:id", mediaCtrl.GetMedia)
		api.DELETE("/:id", mediaCtrl.DeleteMedia)

		// Streaming endpoints
		api.GET("/stream/:id", streamCtrl.StreamMedia)
		api.GET("/stream/:id/thumbnail", streamCtrl.ServeThumbnail)
	}

	return r
}
