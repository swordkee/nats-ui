package handler

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/nats-io/nats.go/jetstream"

	natsclient "nats-ui-backend/internal/nats"
)

type StreamsHandler struct {
	sm *natsclient.ServerManager
}

func NewStreamsHandler(sm *natsclient.ServerManager) *StreamsHandler {
	return &StreamsHandler{sm: sm}
}

func (h *StreamsHandler) getClient(c *gin.Context) (*natsclient.Client, error) {
	serverName := c.Param("server")
	return h.sm.Get(serverName)
}

func (h *StreamsHandler) List(c *gin.Context) {
	nc, err := h.getClient(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 10*time.Second)
	defer cancel()

	var streams []map[string]any
	lister := nc.JS().ListStreams(ctx)
	for info := range lister.Info() {
		streams = append(streams, map[string]any{
			"config": info.Config,
			"state":  info.State,
		})
	}
	if lister.Err() != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": lister.Err().Error()})
		return
	}
	if streams == nil {
		streams = []map[string]any{}
	}
	c.JSON(http.StatusOK, streams)
}

func (h *StreamsHandler) Get(c *gin.Context) {
	nc, err := h.getClient(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	name := c.Param("name")
	stream, err := nc.JS().Stream(ctx, name)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}

	info, err := stream.Info(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, map[string]any{
		"config": info.Config,
		"state":  info.State,
	})
}

type createStreamRequest struct {
	Name        string   `json:"name" binding:"required"`
	Subjects    []string `json:"subjects" binding:"required"`
	Description string   `json:"description"`
	Retention   string   `json:"retention"`
	Storage     string   `json:"storage"`
	MaxMsgs     int64    `json:"maxMsgs"`
	MaxBytes    int64    `json:"maxBytes"`
	MaxAge      int64    `json:"maxAge"` // seconds
	Replicas    int      `json:"replicas"`
}

func (req *createStreamRequest) toStreamConfig() jetstream.StreamConfig {
	cfg := jetstream.StreamConfig{
		Name:        req.Name,
		Subjects:    req.Subjects,
		Description: req.Description,
		MaxMsgs:     req.MaxMsgs,
		MaxBytes:    req.MaxBytes,
		MaxAge:      time.Duration(req.MaxAge) * time.Second,
		Replicas:    req.Replicas,
	}
	switch req.Retention {
	case "interest":
		cfg.Retention = jetstream.InterestPolicy
	case "workqueue":
		cfg.Retention = jetstream.WorkQueuePolicy
	default:
		cfg.Retention = jetstream.LimitsPolicy
	}
	switch req.Storage {
	case "memory":
		cfg.Storage = jetstream.MemoryStorage
	default:
		cfg.Storage = jetstream.FileStorage
	}
	if cfg.Replicas == 0 {
		cfg.Replicas = 1
	}
	return cfg
}

func (h *StreamsHandler) Create(c *gin.Context) {
	var req createStreamRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	nc, err := h.getClient(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	stream, err := nc.JS().CreateStream(ctx, req.toStreamConfig())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	info, err := stream.Info(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusCreated, map[string]any{
		"config": info.Config,
		"state":  info.State,
	})
}

func (h *StreamsHandler) Update(c *gin.Context) {
	var req createStreamRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	nc, err := h.getClient(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	req.Name = c.Param("name")

	stream, err := nc.JS().UpdateStream(ctx, req.toStreamConfig())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	info, err := stream.Info(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, map[string]any{
		"config": info.Config,
		"state":  info.State,
	})
}

type purgeRequest struct {
	Subject string `json:"subject"`
}

func (h *StreamsHandler) Purge(c *gin.Context) {
	nc, err := h.getClient(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	name := c.Param("name")
	stream, err := nc.JS().Stream(ctx, name)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}

	var req purgeRequest
	// Ignore bind errors — body is optional
	_ = c.ShouldBindJSON(&req)

	if req.Subject != "" {
		err = stream.Purge(ctx, jetstream.WithPurgeSubject(req.Subject))
	} else {
		err = stream.Purge(ctx)
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"purged": name, "subject": req.Subject})
}

func (h *StreamsHandler) Delete(c *gin.Context) {
	nc, err := h.getClient(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	name := c.Param("name")
	if err := nc.JS().DeleteStream(ctx, name); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"deleted": name})
}
