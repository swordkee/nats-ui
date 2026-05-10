package handler

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	natsclient "nats-ui-backend/internal/nats"
)

type ServerHandler struct {
	sm *natsclient.ServerManager
}

func NewServerHandler(sm *natsclient.ServerManager) *ServerHandler {
	return &ServerHandler{sm: sm}
}

func (h *ServerHandler) ListServers(c *gin.Context) {
	servers := h.sm.List()
	c.JSON(http.StatusOK, gin.H{"servers": servers})
}

func (h *ServerHandler) getClient(c *gin.Context) (*natsclient.Client, error) {
	serverName := c.Param("server")
	return h.sm.Get(serverName)
}

func (h *ServerHandler) Info(c *gin.Context) {
	nc, err := h.getClient(c)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	data, err := nc.FetchMonitoring("/varz")
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}
	c.Data(http.StatusOK, "application/json", data)
}

func (h *ServerHandler) Connections(c *gin.Context) {
	nc, err := h.getClient(c)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	path := "/connz"
	if subs := c.Query("subs"); subs != "" {
		path += "?subs=" + subs
	}
	data, err := nc.FetchMonitoring(path)
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}
	c.Data(http.StatusOK, "application/json", data)
}

func (h *ServerHandler) JetStreamInfo(c *gin.Context) {
	nc, err := h.getClient(c)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	path := "/jsz"
	q := c.Request.URL.RawQuery
	if q != "" {
		path += "?" + q
	}
	data, err := nc.FetchMonitoring(path)
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}
	c.Data(http.StatusOK, "application/json", data)
}

func (h *ServerHandler) Subscriptions(c *gin.Context) {
	nc, err := h.getClient(c)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	path := "/subsz"
	q := c.Request.URL.RawQuery
	if q != "" {
		path += "?" + q
	}
	data, err := nc.FetchMonitoring(path)
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}
	c.Data(http.StatusOK, "application/json", data)
}

func (h *ServerHandler) Routes(c *gin.Context) {
	nc, err := h.getClient(c)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	data, err := nc.FetchMonitoring("/routez")
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}
	c.Data(http.StatusOK, "application/json", data)
}

func (h *ServerHandler) Health(c *gin.Context) {
	nc, err := h.getClient(c)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"status":    "ok",
		"connected": nc.IsConnected(),
	})
}

func (h *ServerHandler) Gateways(c *gin.Context) {
	nc, err := h.getClient(c)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	data, err := nc.FetchMonitoring("/gatewayz")
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}
	c.Data(http.StatusOK, "application/json", data)
}

func (h *ServerHandler) Leafnodes(c *gin.Context) {
	nc, err := h.getClient(c)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	data, err := nc.FetchMonitoring("/leafz")
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}
	c.Data(http.StatusOK, "application/json", data)
}

func (h *ServerHandler) Accounts(c *gin.Context) {
	nc, err := h.getClient(c)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	data, err := nc.FetchMonitoring("/accountz")
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}
	c.Data(http.StatusOK, "application/json", data)
}

func (h *ServerHandler) AccountDetail(c *gin.Context) {
	nc, err := h.getClient(c)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	account := c.Param("account")
	data, err := nc.FetchMonitoring("/accountz?acc=" + account)
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}
	c.Data(http.StatusOK, "application/json", data)
}

func (h *ServerHandler) ServerVarz(c *gin.Context) {
	nc, err := h.getClient(c)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	data, err := nc.FetchMonitoring("/varz")
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}
	c.Data(http.StatusOK, "application/json", data)
}

func (h *ServerHandler) HealthCheck(c *gin.Context) {
	nc, err := h.getClient(c)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	data, err := nc.FetchMonitoring("/healthz")
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}
	c.Data(http.StatusOK, "application/json", data)
}

// SystemEvents uses Server-Sent Events to stream NATS system events
func (h *ServerHandler) SystemEvents(c *gin.Context) {
	nc, err := h.getClient(c)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	subject := c.DefaultQuery("subject", "$SYS.>")

	sub, ch, err := nc.Subscribe(subject)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	defer sub.Unsubscribe()

	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")
	c.Header("X-Accel-Buffering", "no")

	flusher, _ := c.Writer.(http.Flusher)

	c.Stream(func(w io.Writer) bool {
		select {
		case msg := <-ch:
			data, err := json.Marshal(gin.H{
				"subject":   msg.Subject,
				"data":      string(msg.Data),
				"timestamp": time.Now().UnixMilli(),
			})
			if err != nil {
				fmt.Fprintf(w, "data: {\"error\":\"marshal failed\"}\n\n")
				if flusher != nil {
					flusher.Flush()
				}
				return true
			}
			fmt.Fprintf(w, "data: %s\n\n", data)
			if flusher != nil {
				flusher.Flush()
			}
			return true

		case <-c.Request.Context().Done():
			return false

		case <-time.After(30 * time.Second):
			fmt.Fprintf(w, ": keepalive\n\n")
			if flusher != nil {
				flusher.Flush()
			}
			return true
		}
	})
}
