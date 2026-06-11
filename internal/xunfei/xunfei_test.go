package xunfei

import (
	"encoding/json"
	"net/url"
	"strings"
	"testing"

	"ds2api/internal/promptcompat"
)

func TestBuildAuthURLContainsRequiredParams(t *testing.T) {
	_, u := BuildAuthURL("ai-agent", "/agent/skybox/api/v1/chat", "test_api_key", "test_api_secret")
	if !strings.HasPrefix(u, "wss://ai-agent") {
		t.Fatalf("expected wss:// scheme, got %q", u)
	}
	parsed, err := url.Parse(u)
	if err != nil {
		t.Fatalf("parse url: %v", err)
	}
	q := parsed.Query()
	for _, k := range []string{"host", "date", "authorization"} {
		if q.Get(k) == "" {
			t.Errorf("missing required query param %q in %q", k, u)
		}
	}
	if q.Get("host") != "ai-agent" {
		t.Errorf("host mismatch: got %q", q.Get("host"))
	}
}

func TestDomainForAPIVersion(t *testing.T) {
	cases := map[string]string{
		"v1.1":     "lite",
		"v3.1":     "generalv3",
		"v3.5":     "generalv3.5",
		"v4.0":     "4.0Ultra",
		"unknown":  "generalunknown",
		"v3.5-32K": "max-32k",
		"":         "general",
	}
	for in, want := range cases {
		if got := DomainForAPIVersion(in); got != want {
			t.Errorf("DomainForAPIVersion(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestBuildPayloadOmitsTopKAndIncludesMessages(t *testing.T) {
	stdReq := promptcompat.StandardRequest{
		RequestedModel: "DeepSeek-V3",
		ResolvedModel:  "DeepSeek-V3",
		Messages: []any{
			map[string]any{"role": "system", "content": "you are a helpful assistant"},
			map[string]any{"role": "user", "content": "hello"},
		},
	}
	raw, err := BuildPayload(stdReq, "1", "DeepSeek-V3")
	if err != nil {
		t.Fatalf("BuildPayload: %v", err)
	}
	var got struct {
		Header    map[string]any `json:"header"`
		Parameter struct {
			Chat map[string]any `json:"chat"`
		} `json:"parameter"`
		Payload struct {
			Message struct {
				Text []map[string]any `json:"text"`
			} `json:"message"`
		} `json:"payload"`
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v\npayload: %s", err, string(raw))
	}
	if got.Parameter.Chat["domain"] != "DeepSeek-V3" {
		t.Errorf("domain = %v, want DeepSeek-V3", got.Parameter.Chat["domain"])
	}
	if _, present := got.Parameter.Chat["top_k"]; present {
		t.Errorf("top_k should be omitted, got %v", got.Parameter.Chat["top_k"])
	}
	if len(got.Payload.Message.Text) != 2 {
		t.Fatalf("expected 2 messages, got %d: %+v", len(got.Payload.Message.Text), got.Payload.Message.Text)
	}
	if got.Payload.Message.Text[0]["role"] != "system" {
		t.Errorf("first message role = %v, want system", got.Payload.Message.Text[0]["role"])
	}
	if got.Payload.Message.Text[1]["content"] != "hello" {
		t.Errorf("second message content = %v, want hello", got.Payload.Message.Text[1]["content"])
	}
}

func TestBuildPayloadEmptyDomainFallsBackToModelVersion(t *testing.T) {
	stdReq := promptcompat.StandardRequest{
		RequestedModel: "Spark-Pro",
		ResolvedModel:  "v3.1",
	}
	raw, err := BuildPayload(stdReq, "1", "")
	if err != nil {
		t.Fatalf("BuildPayload: %v", err)
	}
	var got struct {
		Parameter struct {
			Chat map[string]any `json:"chat"`
		} `json:"parameter"`
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Parameter.Chat["domain"] != "generalv3" {
		t.Errorf("expected fallback domain generalv3, got %v", got.Parameter.Chat["domain"])
	}
}

func TestExtractMessageContentHandlesShapes(t *testing.T) {
	cases := []struct {
		in       any
		wantRole string
		wantText string
	}{
		{map[string]any{"role": "system", "content": "sys"}, "system", "sys"},
		{map[string]string{"role": "user", "content": "hi"}, "user", "hi"},
		{"raw string fallback", "user", "raw string fallback"},
		{map[string]any{"role": "assistant"}, "assistant", ""},
		{nil, "user", ""},
	}
	for _, c := range cases {
		got := extractMessageContent(c.in)
		if got.Role != c.wantRole {
			t.Errorf("for %v: role=%q, want %q", c.in, got.Role, c.wantRole)
		}
		if got.Content != c.wantText {
			t.Errorf("for %v: content=%q, want %q", c.in, got.Content, c.wantText)
		}
	}
}

func TestConfigEnabledRequiresThreeFields(t *testing.T) {
	cases := []struct {
		cfg  Config
		want bool
	}{
		{Config{Host: "h", APIKey: "k", APISecret: "s"}, true},
		{Config{Host: "h", APIKey: "k"}, false},
		{Config{Host: "h", APISecret: "s"}, false},
		{Config{APIKey: "k", APISecret: "s"}, false},
		{Config{}, false},
	}
	for i, c := range cases {
		if got := c.cfg.Enabled(); got != c.want {
			t.Errorf("case %d: Enabled()=%v, want %v", i, got, c.want)
		}
	}
}

func TestResolvePathAndDomainPrefersExplicit(t *testing.T) {
	cfg := Config{Path: "/explicit", Domain: "Q"}
	path, domain := resolvePathAndDomain(cfg, promptcompat.StandardRequest{})
	if path != "/explicit" || domain != "Q" {
		t.Errorf("got path=%q domain=%q, want /explicit and Q", path, domain)
	}
}

func TestResolvePathAndDomainDefaultsToSlashV1Chat(t *testing.T) {
	cfg := Config{}
	path, _ := resolvePathAndDomain(cfg, promptcompat.StandardRequest{})
	if path != "/v1/chat" {
		t.Errorf("default path = %q, want /v1/chat", path)
	}
}
