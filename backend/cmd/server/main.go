package main

import (
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"

	"nats-ui-backend/internal/config"
	"nats-ui-backend/internal/handler"
	"nats-ui-backend/internal/middleware"
	natsclient "nats-ui-backend/internal/nats"
)

func main() {
	cfg := config.Load()

	// Connect to NATS servers
	sm, err := natsclient.NewServerManager(cfg)
	if err != nil {
		log.Fatalf("failed to connect to NATS servers: %v", err)
	}
	defer sm.Close()
	log.Printf("connected to %d NATS server(s): %v", len(cfg.Servers), cfg.Servers)

	// Auth
	auth := middleware.NewAuthMiddleware(cfg.JWTSecret)

	// Handlers
	authH := handler.NewAuthHandler(cfg, auth)
	oauth2H := handler.NewOAuth2Handler(cfg, auth)
	serverH := handler.NewServerHandler(sm)
	streamsH := handler.NewStreamsHandler(sm)
	consumersH := handler.NewConsumersHandler(sm)
	kvH := handler.NewKVHandler(sm)
	objH := handler.NewObjectStoreHandler(sm)
	messagesH := handler.NewMessagesHandler(sm)
	benchH := handler.NewBenchHandler(sm)

	// Router
	if os.Getenv("GIN_MODE") == "" {
		gin.SetMode(gin.ReleaseMode)
	}
	r := gin.Default()

	// CORS
	origins := cfg.CORSOriginsList()
	allowCredentials := origins[0] != "*"
	r.Use(cors.New(cors.Config{
		AllowOrigins:     origins,
		AllowMethods:     []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowHeaders:     []string{"Authorization", "Content-Type"},
		AllowCredentials: allowCredentials,
	}))

	// Rate limiting
	rps, _ := strconv.ParseFloat(cfg.RateLimitRPS, 64)
	if rps <= 0 {
		rps = 20
	}
	r.Use(middleware.RateLimit(rps))

	// Validation helpers
	validateServer := middleware.ValidatePathParam("server")
	validateName := middleware.ValidatePathParam("name")
	validateConsumer := middleware.ValidatePathParam("consumer")
	validateBucket := middleware.ValidatePathParam("bucket")
	validateAccount := middleware.ValidatePathParam("account")

	// Public routes
	api := r.Group("/api")
	{
		api.GET("/health", serverH.Health)
		api.POST("/auth/login", authH.Login)
		api.GET("/auth/oauth2/providers", oauth2H.ListProviders)
		api.GET("/auth/oauth2/:provider/authorize", oauth2H.Authorize)
		api.GET("/auth/oauth2/:provider/callback", oauth2H.Callback)
	}

	// Protected routes
	protected := api.Group("", auth.RequireAuth())
	{
		protected.GET("/auth/me", authH.Me)

		// Server list
		protected.GET("/servers", serverH.ListServers)

		// Server monitoring (with server name)
		protected.GET("/servers/:server/info", validateServer, serverH.Info)
		protected.GET("/servers/:server/connections", validateServer, serverH.Connections)
		protected.GET("/servers/:server/jetstream", validateServer, serverH.JetStreamInfo)
		protected.GET("/servers/:server/subscriptions", validateServer, serverH.Subscriptions)
		protected.GET("/servers/:server/routes", validateServer, serverH.Routes)
		protected.GET("/servers/:server/gateways", validateServer, serverH.Gateways)
		protected.GET("/servers/:server/leafnodes", validateServer, serverH.Leafnodes)
		protected.GET("/servers/:server/accounts", validateServer, serverH.Accounts)
		protected.GET("/servers/:server/accounts/:account", validateServer, validateAccount, serverH.AccountDetail)
		protected.GET("/servers/:server/varz", validateServer, serverH.ServerVarz)
		protected.GET("/servers/:server/healthz", validateServer, serverH.HealthCheck)
		protected.GET("/servers/:server/events", validateServer, serverH.SystemEvents)

		// Streams (with server name)
		protected.GET("/servers/:server/streams", validateServer, streamsH.List)
		protected.POST("/servers/:server/streams", validateServer, streamsH.Create)
		protected.GET("/servers/:server/streams/:name", validateServer, validateName, streamsH.Get)
		protected.PUT("/servers/:server/streams/:name", validateServer, validateName, streamsH.Update)
		protected.DELETE("/servers/:server/streams/:name", validateServer, validateName, streamsH.Delete)
		protected.POST("/servers/:server/streams/:name/purge", validateServer, validateName, streamsH.Purge)
		protected.POST("/servers/:server/streams/:name/seal", validateServer, validateName, streamsH.Seal)
		protected.GET("/servers/:server/streams/:name/messages", validateServer, validateName, streamsH.GetMessages)

		// Consumers (with server name)
		protected.GET("/servers/:server/streams/:name/consumers", validateServer, validateName, consumersH.List)
		protected.POST("/servers/:server/streams/:name/consumers", validateServer, validateName, consumersH.Create)
		protected.GET("/servers/:server/streams/:name/consumers/:consumer", validateServer, validateName, validateConsumer, consumersH.Get)
		protected.DELETE("/servers/:server/streams/:name/consumers/:consumer", validateServer, validateName, validateConsumer, consumersH.Delete)
		protected.POST("/servers/:server/streams/:name/consumers/:consumer/pause", validateServer, validateName, validateConsumer, consumersH.Pause)
		protected.POST("/servers/:server/streams/:name/consumers/:consumer/resume", validateServer, validateName, validateConsumer, consumersH.Resume)
		protected.POST("/servers/:server/streams/:name/consumers/:consumer/next", validateServer, validateName, validateConsumer, consumersH.NextMessage)

		// KV Store (with server name)
		protected.GET("/servers/:server/kv", validateServer, kvH.ListBuckets)
		protected.POST("/servers/:server/kv", validateServer, kvH.CreateBucket)
		protected.DELETE("/servers/:server/kv/:bucket", validateServer, validateBucket, kvH.DeleteBucket)
		protected.GET("/servers/:server/kv/:bucket/keys", validateServer, validateBucket, kvH.ListKeys)
		protected.GET("/servers/:server/kv/:bucket/keys/:key", validateServer, validateBucket, kvH.GetValue)
		protected.PUT("/servers/:server/kv/:bucket/keys/:key", validateServer, validateBucket, kvH.PutValue)
		protected.DELETE("/servers/:server/kv/:bucket/keys/:key", validateServer, validateBucket, kvH.DeleteKey)
		protected.GET("/servers/:server/kv/:bucket/watch", validateServer, validateBucket, kvH.WatchKeys)

		// Object Store (with server name)
		protected.GET("/servers/:server/objectstore", validateServer, objH.ListBuckets)
		protected.POST("/servers/:server/objectstore", validateServer, objH.CreateBucket)
		protected.GET("/servers/:server/objectstore/:bucket", validateServer, validateBucket, objH.GetBucket)
		protected.DELETE("/servers/:server/objectstore/:bucket", validateServer, validateBucket, objH.DeleteBucket)
		protected.GET("/servers/:server/objectstore/:bucket/objects", validateServer, validateBucket, objH.ListObjects)
		protected.GET("/servers/:server/objectstore/:bucket/objects/:name", validateServer, validateBucket, validateName, objH.GetObject)
		protected.PUT("/servers/:server/objectstore/:bucket/objects/:name", validateServer, validateBucket, validateName, objH.PutObject)
		protected.DELETE("/servers/:server/objectstore/:bucket/objects/:name", validateServer, validateBucket, validateName, objH.DeleteObject)
		protected.GET("/servers/:server/objectstore/:bucket/objects/:name/info", validateServer, validateBucket, validateName, objH.GetObjectInfo)

		// Messages (with server name)
		protected.POST("/servers/:server/messages/publish", validateServer, messagesH.Publish)
		protected.POST("/servers/:server/messages/request", validateServer, messagesH.RequestReply)
		protected.GET("/servers/:server/messages/subscribe", validateServer, messagesH.Subscribe)
		protected.GET("/servers/:server/messages/subjects", validateServer, messagesH.ActiveSubjects)

		// Benchmark (with server name)
		protected.POST("/servers/:server/bench", validateServer, benchH.Run)
	}

	// SPA fallback for client-side routing
	r.NoRoute(func(c *gin.Context) {
		if _, err := os.Stat("./static/index.html"); err == nil {
			c.File("./static/index.html")
		}
	})

	// Graceful shutdown
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		log.Println("shutting down...")
		sm.Close()
		os.Exit(0)
	}()

	// Build final handler: static files first, then Gin router
	var handler http.Handler = r
	if _, err := os.Stat("./static"); err == nil {
		staticFS := http.Dir("./static")
		fileServer := http.FileServer(staticFS)
		handler = http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			// Try static file first (skip /api routes)
			if !strings.HasPrefix(req.URL.Path, "/api") {
				p := filepath.Clean(req.URL.Path)
				if f, err := staticFS.Open(p); err == nil {
					stat, _ := f.Stat()
					f.Close()
					if stat != nil && !stat.IsDir() {
						fileServer.ServeHTTP(w, req)
						return
					}
				}
			}
			// Fallback to Gin router
			r.ServeHTTP(w, req)
		})
	}

	log.Printf("nats-ui backend listening on :%s", cfg.Port)
	srv := &http.Server{Addr: ":" + cfg.Port, Handler: handler}
	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
