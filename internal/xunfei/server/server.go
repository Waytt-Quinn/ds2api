// Package server provides a thin OpenAI-compatible HTTP front-end
// for the Xunfei WSS backend. It is intentionally separate from
// ds2api's main HTTP server: the main server is built around the
// DeepSeek web API and would require a non-trivial refactor to
// share the chat-completions route with Xunfei. The standalone
// server is the simpler integration point.
package server

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"ds2api/internal/promptcompat"
	"ds2api/internal/toolcall"
	"ds2api/internal/xunfei"
)

// Handler holds the configuration needed to forward OpenAI Chat
// Completions requests to a Xunfei WSS backend.
type Handler struct {
	Cfg xunfei.Config
}

// NewHandler builds a Handler. If cfg.Enabled() is false the
// server still starts but every request returns 503; this lets
// the operator boot the binary before xunfei config is in place.
func NewHandler(cfg xunfei.Config) *Handler {
	return &Handler{Cfg: cfg}
}

// emptyConfigReader satisfies promptcompat.ConfigReader with no
// model aliases. Used because the standalone xunfei server doesn't
// have access to ds2api's main config store; the upstream domain
// override is what matters for routing.
type emptyConfigReader struct{}

func (emptyConfigReader) ModelAliases() map[string]string { return nil }

// RegisterRoutes wires the handler onto an http.ServeMux.
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/v1/chat/completions", h.ChatCompletions)
	mux.HandleFunc("/v1/models", h.ListModels)
	mux.HandleFunc("/healthz", h.Healthz)
}

// ChatCompletions accepts an OpenAI Chat Completions request and
// forwards it to the Xunfei WSS backend. Both streaming and
// non-streaming modes are supported.
func (h *Handler) ChatCompletions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !h.Cfg.Enabled() {
		http.Error(w, "xunfei backend not configured", http.StatusServiceUnavailable)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		http.Error(w, "read body: "+err.Error(), http.StatusBadRequest)
		return
	}
	var req map[string]any
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "invalid json: "+err.Error(), http.StatusBadRequest)
		return
	}
	// Minimal stub ConfigReader: the standalone xunfei server
	// doesn't know about the main ds2api store, but it still
	// needs to resolve model aliases for normalize.
	stdReq, err := promptcompat.NormalizeOpenAIChatRequest(emptyConfigReader{}, req, "")
	if err != nil {
		http.Error(w, "normalize: "+err.Error(), http.StatusBadRequest)
		return
	}

	src, err := xunfei.Completion(r.Context(), h.Cfg, stdReq)
	if err != nil {
		http.Error(w, "xunfei completion: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer src.Close()

	if stdReq.Stream {
		h.streamResponse(w, r, src, stdReq)
		return
	}
	h.collectResponse(w, r, src, stdReq)
}

// collectResponse drains the WSS stream into one tool-call turn
// and returns a single non-streaming OpenAI Chat Completions JSON.
func (h *Handler) collectResponse(w http.ResponseWriter, r *http.Request, src xunfei.Source, stdReq promptcompat.StandardRequest) {
	var visibleBuilder strings.Builder
	var lastFrame xunfei.Frame
	for {
		f, err := src.Next(r.Context())
		if err != nil {
			if err == io.EOF {
				break
			}
			http.Error(w, "xunfei read: "+err.Error(), http.StatusBadGateway)
			return
		}
		lastFrame = f
		visibleBuilder.WriteString(f.Content)
	}
	if lastFrame.ErrorCode != 0 {
		http.Error(w, fmt.Sprintf("xunfei upstream error %d: %s", lastFrame.ErrorCode, lastFrame.ErrorMsg), http.StatusBadGateway)
		return
	}
	visible := visibleBuilder.String()
	calls := toolcall.ParseToolCalls(visible, stdReq.ToolNames)
	body := map[string]any{
		"id":      "chatcmpl-" + stdReq.ResponseModel,
		"object":  "chat.completion",
		"created": time.Now().Unix(),
		"model":   stdReq.ResponseModel,
		"choices": []map[string]any{
			{
				"index": 0,
				"message": map[string]any{
					"role":    "assistant",
					"content": visible,
				},
				"finish_reason": "stop",
			},
		},
		"usage": map[string]any{
			"prompt_tokens":     0,
			"completion_tokens": 0,
			"total_tokens":      0,
		},
	}
	if len(calls) > 0 {
		body["choices"].([]map[string]any)[0]["message"].(map[string]any)["tool_calls"] = callsToOpenAI(calls)
		body["choices"].([]map[string]any)[0]["finish_reason"] = "tool_calls"
	}
	writeJSON(w, http.StatusOK, body)
}

// streamResponse writes SSE chunks to the client as they arrive
// from the WSS stream. Each upstream frame is sent as one
// content_delta, and any tool calls extracted from the cumulative
// text are emitted as a single tool_calls delta on the final frame.
func (h *Handler) streamResponse(w http.ResponseWriter, r *http.Request, src xunfei.Source, stdReq promptcompat.StandardRequest) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	flusher, _ := w.(http.Flusher)

	var visibleBuilder strings.Builder
	var lastFrame xunfei.Frame
	for {
		f, err := src.Next(r.Context())
		if err != nil {
			if err == io.EOF {
				break
			}
			// Emit a final error chunk so the client knows.
			writeSSE(w, "error", map[string]any{"message": err.Error()})
			if flusher != nil {
				flusher.Flush()
			}
			return
		}
		lastFrame = f
		visibleBuilder.WriteString(f.Content)
		writeSSE(w, "", map[string]any{
			"id":      "chatcmpl-stream",
			"object":  "chat.completion.chunk",
			"created": time.Now().Unix(),
			"model":   stdReq.ResponseModel,
			"choices": []map[string]any{
				{
					"index": 0,
					"delta": map[string]any{"content": f.Content},
				},
			},
		})
		if flusher != nil {
			flusher.Flush()
		}
	}
	if lastFrame.ErrorCode != 0 {
		writeSSE(w, "error", map[string]any{
			"message": fmt.Sprintf("xunfei upstream error %d: %s", lastFrame.ErrorCode, lastFrame.ErrorMsg),
		})
		if flusher != nil {
			flusher.Flush()
		}
		return
	}
	visible := visibleBuilder.String()
	calls := toolcall.ParseToolCalls(visible, stdReq.ToolNames)
	finish := "stop"
	if len(calls) > 0 {
		writeSSE(w, "", map[string]any{
			"id":      "chatcmpl-stream",
			"object":  "chat.completion.chunk",
			"created": time.Now().Unix(),
			"model":   stdReq.ResponseModel,
			"choices": []map[string]any{
				{
					"index": 0,
					"delta": map[string]any{
						"role":       "assistant",
						"tool_calls": callsToOpenAI(calls),
					},
				},
			},
		})
		finish = "tool_calls"
	}
	writeSSE(w, "", map[string]any{
		"id":      "chatcmpl-stream",
		"object":  "chat.completion.chunk",
		"created": time.Now().Unix(),
		"model":   stdReq.ResponseModel,
		"choices": []map[string]any{
			{"index": 0, "delta": map[string]any{}, "finish_reason": finish},
		},
	})
	writeSSE(w, "done", "[DONE]")
	if flusher != nil {
		flusher.Flush()
	}
}

// ListModels returns a minimal model list. The Xunfei backend
// only routes via domain, not model name, so the list is a
// single placeholder.
func (h *Handler) ListModels(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"object": "list",
		"data": []map[string]any{
			{"id": "xunfei", "object": "model", "created": time.Now().Unix(), "owned_by": "xunfei"},
		},
	})
}

// Healthz reports whether the Xunfei backend is configured.
func (h *Handler) Healthz(w http.ResponseWriter, _ *http.Request) {
	if h.Cfg.Enabled() {
		writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
		return
	}
	writeJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "xunfei not configured"})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeSSE(w http.ResponseWriter, event string, data any) {
	if event == "done" {
		fmt.Fprintf(w, "data: %s\n\n", data)
		return
	}
	if event != "" {
		fmt.Fprintf(w, "event: %s\n", event)
	}
	b, _ := json.Marshal(data)
	fmt.Fprintf(w, "data: %s\n\n", b)
}

// callsToOpenAI converts the toolcall package's ParsedToolCall
// model into the OpenAI tool_calls wire format (id, type, function).
// The ds2api parser exposes parsed arguments as a map[string]any;
// the OpenAI wire format expects a JSON-encoded string, so we
// re-encode the map.
func callsToOpenAI(calls []toolcall.ParsedToolCall) []map[string]any {
	out := make([]map[string]any, 0, len(calls))
	for i, c := range calls {
		args := c.Input
		var argsStr string
		if args == nil {
			argsStr = "{}"
		} else {
			b, _ := json.Marshal(args)
			argsStr = string(b)
		}
		out = append(out, map[string]any{
			"id":       fmt.Sprintf("call_%d", i),
			"type":     "function",
			"function": map[string]any{"name": c.Name, "arguments": argsStr},
		})
	}
	return out
}
