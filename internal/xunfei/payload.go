package xunfei

import (
	"encoding/json"

	"ds2api/internal/promptcompat"
)

// Tool shape mirrors the OpenAI function-calling schema that the
// ai-agent gateway expects. Some private gateways (e.g. one
// proxying DeepSeek-V3) expect this shape rather than the legacy
// xunfei functions.text format.
type Tool struct {
	Type     string   `json:"type,omitempty"`
	Function Function `json:"function"`
}

type Function struct {
	Name        string `json:"name,omitempty"`
	Description string `json:"description,omitempty"`
	Parameters  any    `json:"parameters,omitempty"`
}

type message struct {
	Role        string `json:"role,omitempty"`
	Content     string `json:"content"`
	ContentType string `json:"content_type,omitempty"`
}

type payloadMessages struct {
	Text []message `json:"text"`
}

type chatParameter struct {
	Domain         string   `json:"domain,omitempty"`
	Temperature    *float64 `json:"temperature,omitempty"`
	TopK           int      `json:"top_k,omitempty"`
	MaxTokens      int      `json:"max_tokens,omitempty"`
	Auditing       bool     `json:"auditing,omitempty"`
	ContextEnabled *bool    `json:"contextEnabled,omitempty"`
}

type requestHeader struct {
	AppID   string `json:"app_id,omitempty"`
	TraceID string `json:"traceId,omitempty"`
	Mode    int    `json:"mode,omitempty"`
}

type requestParameter struct {
	Chat chatParameter `json:"chat"`
}

type requestPayload struct {
	SessionID string          `json:"sessionId,omitempty"`
	Message   payloadMessages `json:"message"`
	Tools     []Tool          `json:"tools,omitempty"`
}

type chatRequest struct {
	Header    requestHeader    `json:"header"`
	Parameter requestParameter `json:"parameter"`
	Payload   requestPayload   `json:"payload"`
}

// messageContent is a tolerant view of promptcompat's per-message
// shape. The Messages field on StandardRequest is []any because the
// normalisation layer carries both raw text and richer content
// types; for Xunfei we only need role + text.
type messageContent struct {
	Role    string
	Content string
}

// BuildPayload turns a promptcompat.StandardRequest into the
// Xunfei WSS payload format. The standard request's FinalPrompt
// already contains the DSML tool-call instructions injected by
// promptcompat (when tools are present), so the Xunfei-side parser
// will see the model emit DSML blocks in its response.
//
// domain overrides the per-version default (use the XUNFEI_DOMAIN
// env var for the ai-agent gateway's DeepSeek-V3 routing).
func BuildPayload(stdReq promptcompat.StandardRequest, appID, domain string) ([]byte, error) {
	if domain == "" {
		domain = DomainForAPIVersion(stdReq.ResolvedModel)
		if domain == "general" {
			domain = DomainForAPIVersion(stdReq.RequestedModel)
		}
	}

	messages := make([]message, 0, len(stdReq.Messages))
	for _, raw := range stdReq.Messages {
		mc := extractMessageContent(raw)
		if mc.Content == "" {
			continue
		}
		messages = append(messages, message{
			Role:        mc.Role,
			Content:     mc.Content,
			ContentType: "text",
		})
	}

	req := chatRequest{
		Header: requestHeader{
			AppID: appID,
			Mode:  0,
		},
		Parameter: requestParameter{
			Chat: chatParameter{
				Domain:    domain,
				MaxTokens: 0, // Xunfei v1 doesn't carry max_tokens from the standard request.
			},
		},
		Payload: requestPayload{
			SessionID: "",
			Message:   payloadMessages{Text: messages},
		},
	}

	return json.Marshal(req)
}

func extractMessageContent(raw any) messageContent {
	mc := messageContent{Role: "user"}
	switch v := raw.(type) {
	case map[string]any:
		if r, ok := v["role"].(string); ok {
			mc.Role = r
		}
		if c, ok := v["content"].(string); ok {
			mc.Content = c
		}
	case map[string]string:
		mc.Role = v["role"]
		mc.Content = v["content"]
	default:
		// Last-ditch: stringify. The Xunfei gateway expects text.
		if s, ok := raw.(string); ok {
			mc.Content = s
		}
	}
	return mc
}
