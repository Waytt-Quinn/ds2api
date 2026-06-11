package xunfei

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"ds2api/internal/promptcompat"
)

// startFakeWS upgrades an httptest.Server to a WebSocket endpoint
// and returns the server's WSS URL plus a channel of inbound
// messages the client sent.
func startFakeWS(t *testing.T, framesToSend [][]byte) (string, <-chan []byte) {
	t.Helper()
	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}
	inbound := make(chan []byte, 16)
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		defer c.Close()
		// Read the one request the client sends.
		_, msg, err := c.ReadMessage()
		if err != nil {
			t.Errorf("read request: %v", err)
			return
		}
		inbound <- msg
		// Stream the canned frames back.
		for _, f := range framesToSend {
			if err := c.WriteMessage(websocket.TextMessage, f); err != nil {
				return
			}
		}
	}))
	wssURL := "wss" + strings.TrimPrefix(srv.URL, "https") + "/ws"
	return wssURL, inbound
}

func frameJSON(status int, content string) []byte {
	payload := map[string]any{
		"header": map[string]any{
			"code":    0,
			"message": "success",
			"sid":     "TEST",
			"status":  status,
		},
		"payload": map[string]any{
			"choices": map[string]any{
				"status": status,
				"text": []map[string]any{
					{"content": content, "role": "assistant", "index": 0, "content_type": "text"},
				},
			},
		},
	}
	b, _ := json.Marshal(payload)
	return b
}

func TestCompletionEndToEnd(t *testing.T) {
	wssURL, inbound := startFakeWS(t, [][]byte{
		frameJSON(0, ""),
		frameJSON(1, "Hello "),
		frameJSON(1, "world"),
		frameJSON(2, ""),
	})

	// Convert the test WSS URL into a host+path the xunfei
	// dialer understands: split off "/ws" as the path, the rest
	// is the host:port.
	idx := strings.Index(wssURL, "/ws")
	hostPort := strings.TrimPrefix(wssURL[:idx], "wss://")
	path := wssURL[idx:]

	cfg := Config{
		Host:               hostPort,
		Path:               path,
		APIKey:             "k",
		APISecret:          "s",
		Domain:             "test",
		InsecureSkipVerify: true,
		HandshakeTimeout:   2 * time.Second,
	}
	stdReq := promptcompat.StandardRequest{
		RequestedModel: "test",
		ResolvedModel:  "test",
		Messages: []any{
			map[string]any{"role": "user", "content": "hi"},
		},
	}

	src, err := Completion(context.Background(), cfg, stdReq)
	if err != nil {
		_, authURL := BuildAuthURL(cfg.Host, cfg.Path, cfg.APIKey, cfg.APISecret)
		t.Fatalf("Completion: %v\nauth URL: %q", err, authURL)
	}
	defer src.Close()

	// Drain the inbound channel to confirm the request was sent.
	select {
	case got := <-inbound:
		if len(got) == 0 {
			t.Errorf("empty inbound request")
		}
		if !strings.Contains(string(got), `"domain":"test"`) {
			t.Errorf("inbound request missing domain: %s", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for inbound request")
	}

	// Drain frames.
	got := []string{}
	for i := 0; i < 4; i++ {
		f, err := src.Next(context.Background())
		if err != nil {
			t.Fatalf("frame %d: %v", i, err)
		}
		got = append(got, f.Content)
	}
	// The 4th call should return io.EOF.
	if _, err := src.Next(context.Background()); err == nil {
		t.Errorf("expected io.EOF on 5th frame, got nil")
	}

	want := []string{"", "Hello ", "world", ""}
	for i, w := range want {
		if got[i] != w {
			t.Errorf("frame %d content = %q, want %q", i, got[i], w)
		}
	}
}

func TestCompletionRejectsUnconfiguredConfig(t *testing.T) {
	_, err := Completion(context.Background(), Config{}, promptcompat.StandardRequest{})
	if err == nil {
		t.Errorf("expected error for empty config")
	}
}
