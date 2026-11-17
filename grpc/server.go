package grpc

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"streamly/media-service/config"
	"streamly/media-service/models"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"gorm.io/gorm"
)

type MediaMessage struct {
	ID           string  `json:"id"`
	Title        string  `json:"title"`
	Description  string  `json:"description"`
	MediaType    string  `json:"media_type"`
	Artist       string  `json:"artist"`
	Duration     float64 `json:"duration"`
	StreamURL    string  `json:"stream_url"`
	ThumbnailURL string  `json:"thumbnail_url"`
	MimeType     string  `json:"mime_type"`
	FileSize     int64   `json:"file_size"`
	UploaderID   string  `json:"uploader_id"`
	UploaderName string  `json:"uploader_name"`
	Tags         string  `json:"tags"`
	Status       string  `json:"status"`
	CreatedAt    string  `json:"created_at"`
}

type Server struct {
	db  *gorm.DB
	cfg config.Config
}

func NewServer(db *gorm.DB, cfg config.Config) *Server {
	return &Server{db: db, cfg: cfg}
}

func (s *Server) Start(port string) error {
	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return fmt.Errorf("failed to listen on port %s: %v", port, err)
	}

	grpcServer := grpc.NewServer()

	// Register unary handlers using gRPC ServiceDesc
	serviceDesc := grpc.ServiceDesc{
		ServiceName: "media.MediaService",
		HandlerType: (*any)(nil),
		Methods: []grpc.MethodDesc{
			{
				MethodName: "GetMedia",
				Handler: func(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
					in := new(struct{ ID string })
					if err := dec(in); err != nil {
						return nil, err
					}
					var media models.Media
					if err := s.db.First(&media, "id = ?", in.ID).Error; err != nil {
						return nil, fmt.Errorf("media not found: %v", err)
					}
					return map[string]interface{}{
						"media": toProtoMessage(&media),
					}, nil
				},
			},
			{
				MethodName: "ListMedia",
				Handler: func(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
					in := new(struct {
						MediaType  string
						UploaderID string
						Limit      int
					})
					if err := dec(in); err != nil {
						return nil, err
					}
					query := s.db.Order("created_at DESC")
					if in.MediaType != "" && in.MediaType != "all" {
						query = query.Where("media_type = ?", in.MediaType)
					}
					if in.UploaderID != "" {
						query = query.Where("uploader_id = ?", in.UploaderID)
					}
					limit := in.Limit
					if limit <= 0 || limit > 50 {
						limit = 50
					}
					var list []models.Media
					if err := query.Limit(limit).Find(&list).Error; err != nil {
						return nil, err
					}
					items := make([]MediaMessage, len(list))
					for i, m := range list {
						items[i] = toProtoMessage(&m)
					}
					return map[string]interface{}{
						"items": items,
						"count": len(items),
					}, nil
				},
			},
		},
	}

	grpcServer.RegisterService(&serviceDesc, s)

	log.Printf("media-service gRPC server listening on port %s", port)
	return grpcServer.Serve(lis)
}

func toProtoMessage(m *models.Media) MediaMessage {
	return MediaMessage{
		ID:           m.ID,
		Title:        m.Title,
		Description:  m.Description,
		MediaType:    string(m.MediaType),
		Artist:       m.Artist,
		Duration:     m.Duration,
		StreamURL:    m.StreamURL,
		ThumbnailURL: m.ThumbnailURL,
		MimeType:     m.MimeType,
		FileSize:     m.FileSize,
		UploaderID:   m.UploaderID,
		UploaderName: m.UploaderName,
		Tags:         m.Tags,
		Status:       m.Status,
		CreatedAt:    m.CreatedAt.Format(time.RFC3339),
	}
}

// Client helper: calls search-service via gRPC to index newly uploaded media
func IndexMediaInSearchServiceGRPC(searchServiceAddr string, m *models.Media) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, searchServiceAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return fmt.Errorf("failed to connect to search-service gRPC at %s: %v", searchServiceAddr, err)
	}
	defer conn.Close()

	doc := toProtoMessage(m)
	var out struct {
		Success bool
		ID      string
	}
	err = conn.Invoke(ctx, "/search.SearchService/IndexMedia", map[string]interface{}{"document": doc}, &out)
	if err != nil {
		return fmt.Errorf("gRPC IndexMedia call failed: %v", err)
	}
	log.Printf("Successfully indexed media %s via gRPC in search-service", m.ID)
	return nil
}
