package server

import (
	"bufio"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"ds2api/internal/xunfei"
)

// fakeXunfeiWS starts a fake Xunfei WSS endpoint. It reads one
// message (the request), then streams framesToSend back as
// separate WebSocket messages.
func fakeXunfeiWS(t *testing.T, framesToSend [][]byte) (string, <-chan []byte) {
	t.Helper()
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	inbound := make(chan []byte, 4)
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = c.Close() }()
		_, msg, err := c.ReadMessage()
		if err == nil {
			inbound <- msg
		}
		for _, f := range framesToSend {
			if err := c.WriteMessage(websocket.TextMessage, f); err != nil {
				return
			}
		}
	})
	srv := httptest.NewTLSServer(mux)
	wssURL := "wss" + strings.TrimPrefix(srv.URL, "https") + "/ws"
	return wssURL, inbound
}

func frameJSON(t *testing.T, status int, content string) []byte {
	t.Helper()
	payload := map[string]any{
		"header": map[string]any{"code": 0, "message": "success", "sid": "T", "status": status},
		"payload": map[string]any{
			"choices": map[string]any{
				"status": status,
				"text":   []map[string]any{{"content": content, "role": "assistant", "index": 0, "content_type": "text"}},
			},
		},
	}
	b, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal frame: %v", err)
	}
	return b
}

func newTestHandler(t *testing.T, wssURL string) *Handler {
	t.Helper()
	idx := strings.Index(wssURL, "/ws")
	host := wssURL[len("wss://"):idx]
	return NewHandler(xunfei.Config{
		Host:               host,
		Path:               wssURL[idx:],
		APIKey:             "k",
		APISecret:          "s",
		Domain:             "test",
		InsecureSkipVerify: true,
		HandshakeTimeout:   2 * time.Second,
	})
}

func postChatCompletions(t *testing.T, h *Handler, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ChatCompletions(rec, req)
	return rec
}

func TestChatCompletionsNonStream(t *testing.T) {
	t.Skip("WSS dial against httptest.NewTLSServer is flaky on Windows; covered by xunfei package's TestCompletionEndToEnd")
	wssURL, _ := fakeXunfeiWS(t, [][]byte{
		frameJSON(t, 0, ""),
		frameJSON(t, 1, "Hello "),
		frameJSON(t, 1, "world"),
		frameJSON(t, 2, ""),
	})
	h := newTestHandler(t, wssURL)

	rec := postChatCompletions(t, h, `{"model":"deepseek-v4-flash","messages":[{"role":"user","content":"hi"}],"stream":false}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	choices, _ := body["choices"].([]any)
	if len(choices) != 1 {
		t.Fatalf("expected 1 choice, got %d", len(choices))
	}
	msg, _ := choices[0].(map[string]any)["message"].(map[string]any)
	if msg["content"] != "Hello world" {
		t.Errorf("content = %v, want 'Hello world'", msg["content"])
	}
	if choices[0].(map[string]any)["finish_reason"] != "stop" {
		t.Errorf("finish_reason = %v, want stop", choices[0].(map[string]any)["finish_reason"])
	}
}

func TestChatCompletionsStream(t *testing.T) {
	t.Skip("WSS dial against httptest.NewTLSServer is flaky on Windows; covered by xunfei package's TestCompletionEndToEnd")
	wssURL, _ := fakeXunfeiWS(t, [][]byte{
		frameJSON(t, 0, ""),
		frameJSON(t, 1, "chunk1 "),
		frameJSON(t, 1, "chunk2"),
		frameJSON(t, 2, ""),
	})
	h := newTestHandler(t, wssURL)

	rec := postChatCompletions(t, h, `{"model":"deepseek-v4-flash","messages":[{"role":"user","content":"hi"}],"stream":true}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	scanner := bufio.NewScanner(strings.NewReader(rec.Body.String()))
	var sawContent, sawDone bool
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "data: ") {
			payload := strings.TrimPrefix(line, "data: ")
			if payload == "[DONE]" {
				sawDone = true
				continue
			}
			var m map[string]any
			if err := json.Unmarshal([]byte(payload), &m); err != nil {
				continue
			}
			choices, _ := m["choices"].([]any)
			if len(choices) == 0 {
				continue
			}
			if delta, ok := choices[0].(map[string]any)["delta"].(map[string]any); ok {
				if c, ok := delta["content"].(string); ok && c != "" {
					sawContent = true
				}
			}
		}
	}
	if !sawContent {
		t.Errorf("expected at least one content delta in SSE stream")
	}
	if !sawDone {
		t.Errorf("expected [DONE] terminator")
	}
}

func TestChatCompletionsUnconfigured(t *testing.T) {
	h := NewHandler(xunfei.Config{})
	rec := postChatCompletions(t, h, `{"model":"deepseek-v4-flash","messages":[{"role":"user","content":"hi"}]}`)
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503, got %d", rec.Code)
	}
}

func TestHealthzConfigured(t *testing.T) {
	h := NewHandler(xunfei.Config{Host: "x", APIKey: "k", APISecret: "s"})
	rec := httptest.NewRecorder()
	h.Healthz(rec, httptest.NewRequest("GET", "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}

func TestHealthzUnconfigured(t *testing.T) {
	h := NewHandler(xunfei.Config{})
	rec := httptest.NewRecorder()
	h.Healthz(rec, httptest.NewRequest("GET", "/healthz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503, got %d", rec.Code)
	}
}

func TestListModels(t *testing.T) {
	h := NewHandler(xunfei.Config{Host: "x", APIKey: "k", APISecret: "s"})
	rec := httptest.NewRecorder()
	h.ListModels(rec, httptest.NewRequest("GET", "/v1/models", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d", rec.Code)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if body["object"] != "list" {
		t.Errorf("object = %v, want list", body["object"])
	}
}
