package main

import (
	"log"

	"streamly/media-service/config"
	mediagrpc "streamly/media-service/grpc"
	mediakafka "streamly/media-service/kafka"
	"streamly/media-service/routes"
)

func main() {
	cfg := config.LoadConfig()
	db := config.InitDB(cfg)
	rdb := config.InitRedis(cfg)

	// Initialize Kafka Producer
	kafkaProducer := mediakafka.NewProducer(cfg.KafkaBrokers)
	defer kafkaProducer.Close()

	// Start gRPC server in background
	grpcServer := mediagrpc.NewServer(db, cfg)
	go func() {
		if err := grpcServer.Start(cfg.GRPCPort); err != nil {
			log.Printf("media-service gRPC server error: %v", err)
		}
	}()

	// Start Gin HTTP Server
	r := routes.SetupRouter(db, rdb, kafkaProducer, cfg)

	log.Printf("media-service HTTP running on port %s, gRPC on port %s", cfg.Port, cfg.GRPCPort)
	if err := r.Run(":" + cfg.Port); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}
