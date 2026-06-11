// Command ds2api-xunfei runs a standalone OpenAI-compatible HTTP
// server backed by the Xunfei WSS protocol. It is intentionally
// separate from the main ds2api binary: the main binary routes
// through chat.deepseek.com (the DeepSeek web API) and would
// require a non-trivial refactor to share its chat-completions
// route. The standalone server is the simpler integration point
// for users who have an internal Xunfei gateway.
//
// Configuration via env vars (mirror of the one-api xunfei
// provider's env vars):
//
//	XUNFEI_API_HOST             (required) e.g. "ai-agent"
//	XUNFEI_API_PATH             (optional) e.g. "/agent/skybox/api/v1/chat"
//	XUNFEI_API_PATH_PREFIX      (optional) e.g. "/agent/skybox"
//	XUNFEI_API_KEY              (required) app id
//	XUNFEI_API_SECRET           (required) HMAC-SHA256 secret
//	XUNFEI_DOMAIN               (optional) e.g. "DeepSeek-V3"
//	XUNFEI_COOKIE               (optional) cookie header for the WSS handshake
//	XUNFEI_INSECURE_SKIP_VERIFY (optional) "true" to skip TLS verify
//	XUNFEI_CONTEXT_ENABLED      (optional) "true" to enable context
//	DS2API_XUNFEI_PORT          (optional) listen port (default 5002)
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"ds2api/internal/xunfei"
	"ds2api/internal/xunfei/server"
)

func main() {
	if err := main2(); err != nil {
		log.Fatalf("ds2api-xunfei: %v", err)
	}
}

func main2() error {
	cfg := xunfei.Config{
		Host:               os.Getenv("XUNFEI_API_HOST"),
		Path:               os.Getenv("XUNFEI_API_PATH"),
		PathPrefix:         os.Getenv("XUNFEI_API_PATH_PREFIX"),
		APIKey:             os.Getenv("XUNFEI_API_KEY"),
		APISecret:          os.Getenv("XUNFEI_API_SECRET"),
		Domain:             os.Getenv("XUNFEI_DOMAIN"),
		Cookie:             os.Getenv("XUNFEI_COOKIE"),
		InsecureSkipVerify: strings.EqualFold(os.Getenv("XUNFEI_INSECURE_SKIP_VERIFY"), "true"),
		HandshakeTimeout:   5 * time.Second,
	}
	if !cfg.Enabled() {
		log.Printf("WARNING: XUNFEI_API_HOST or XUNFEI_API_KEY/XUNFEI_API_SECRET not set. The server will start but every request will return 503.")
	}

	port := os.Getenv("DS2API_XUNFEI_PORT")
	if port == "" {
		port = "5002"
	}
	if _, err := strconv.Atoi(port); err != nil {
		return fmt.Errorf("invalid DS2API_XUNFEI_PORT %q: %w", port, err)
	}

	mux := http.NewServeMux()
	handler := server.NewHandler(cfg)
	handler.RegisterRoutes(mux)

	srv := &http.Server{
		Addr:              "0.0.0.0:" + port,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	idleDone := make(chan struct{})
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		log.Printf("shutting down")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
		close(idleDone)
	}()

	log.Printf("ds2api-xunfei listening on %s (xunfei host=%q, domain=%q)", srv.Addr, cfg.Host, cfg.Domain)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	<-idleDone
	return nil
}
