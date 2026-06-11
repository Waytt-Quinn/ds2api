package xunfei

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/websocket"

	"ds2api/internal/promptcompat"
)

// Config holds the connection settings for the Xunfei WSS backend.
// The zero value is invalid: Host or Path must be set for the
// backend to be enabled.
type Config struct {
	Host               string
	Path               string
	PathPrefix         string
	APIKey             string
	APISecret          string
	Domain             string
	Cookie             string
	InsecureSkipVerify bool
	HandshakeTimeout   time.Duration
}

// Enabled reports whether the config has the minimum fields
// required to dial the WSS backend. When false, callers should
// fall back to the deepseek web backend.
func (c Config) Enabled() bool {
	return strings.TrimSpace(c.Host) != "" && strings.TrimSpace(c.APIKey) != "" && strings.TrimSpace(c.APISecret) != ""
}

// Frame is one decoded Xunfei WSS response frame. The upstream
// protocol returns a JSON envelope with payload.choices.text[]
// carrying incremental content; status 2 marks end-of-stream.
type Frame struct {
	Status    int    `json:"status"`
	Content   string `json:"content"`
	ErrorCode int    `json:"errorCode"`
	ErrorMsg  string `json:"errorMsg"`
}

// rawResponse mirrors the upstream Xunfei JSON shape.
type rawResponse struct {
	Header struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		Sid     string `json:"sid"`
		Status  int    `json:"status"`
	} `json:"header"`
	Payload struct {
		Choices struct {
			Status int `json:"status"`
			Seq    int `json:"seq"`
			Text   []struct {
				Content      string         `json:"content"`
				Role         string         `json:"role"`
				Index        int            `json:"index"`
				ContentType  string         `json:"content_type"`
				FunctionCall *modelFunction `json:"function_call"`
			} `json:"text"`
		} `json:"choices"`
	} `json:"payload"`
}

type modelFunction struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

// Source yields decoded frames from a live Xunfei WSS connection.
// The first frame is the trailing handshake ack (status=0,
// empty); subsequent frames carry content (status=1) until the
// final status=2 frame.
type Source interface {
	// Next returns the next frame. io.EOF marks clean end-of-stream.
	Next(ctx context.Context) (Frame, error)
	Close() error
}

// Completion opens a WSS connection, sends the Xunfei payload, and
// returns a Source that yields incremental content frames.
func Completion(ctx context.Context, cfg Config, stdReq promptcompat.StandardRequest) (Source, error) {
	if !cfg.Enabled() {
		return nil, errors.New("xunfei backend not configured")
	}
	authPath, domain := resolvePathAndDomain(cfg, stdReq)
	_, authURL := BuildAuthURL(cfg.Host, authPath, cfg.APIKey, cfg.APISecret)

	handshake := cfg.HandshakeTimeout
	if handshake == 0 {
		handshake = 5 * time.Second
	}
	dialer := websocket.Dialer{
		HandshakeTimeout: handshake,
		TLSClientConfig:  &tls.Config{InsecureSkipVerify: cfg.InsecureSkipVerify},
	}
	var requestHeader http.Header
	if cfg.Cookie != "" {
		requestHeader = http.Header{}
		requestHeader.Set("Cookie", cfg.Cookie)
	}
	conn, resp, err := dialer.Dial(authURL, requestHeader)
	if err != nil {
		return nil, fmt.Errorf("xunfei wss dial %s: %w", authURL, err)
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		_ = conn.Close()
		return nil, fmt.Errorf("xunfei wss upgrade failed: status=%d", resp.StatusCode)
	}

	payload, err := BuildPayload(stdReq, "1", domain)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("xunfei build payload: %w", err)
	}
	if err := conn.WriteMessage(websocket.TextMessage, payload); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("xunfei write payload: %w", err)
	}

	return &wssSource{conn: conn}, nil
}

// resolvePathAndDomain picks the WSS path and the upstream domain
// for the given request. Mirrors getXunfeiAuthUrl in one-api but
// simplified: the api-agent gateway uses a single path override
// (cfg.Path) and a single domain override (cfg.Domain); the
// per-version path/prefix logic is not needed.
func resolvePathAndDomain(cfg Config, _ promptcompat.StandardRequest) (path, domain string) {
	if cfg.Path != "" {
		path = cfg.Path
	} else if cfg.PathPrefix != "" {
		path = cfg.PathPrefix + "/v1/chat"
	} else {
		path = "/v1/chat"
	}
	domain = cfg.Domain
	return
}

type wssSource struct {
	conn *websocket.Conn
}

func (s *wssSource) Next(ctx context.Context) (Frame, error) {
	type result struct {
		msg []byte
		err error
	}
	ch := make(chan result, 1)
	go func() {
		_, msg, err := s.conn.ReadMessage()
		ch <- result{msg: msg, err: err}
	}()
	select {
	case <-ctx.Done():
		return Frame{}, ctx.Err()
	case r := <-ch:
		if r.err != nil {
			if websocket.IsCloseError(r.err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				return Frame{}, io.EOF
			}
			return Frame{}, r.err
		}
		return decodeFrame(r.msg)
	}
}

func (s *wssSource) Close() error {
	return s.conn.Close()
}

// decodeFrame parses one WSS payload into a Frame.
func decodeFrame(raw []byte) (Frame, error) {
	var resp rawResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		return Frame{}, fmt.Errorf("xunfei decode frame: %w", err)
	}
	frame := Frame{
		Status:    resp.Payload.Choices.Status,
		ErrorCode: resp.Header.Code,
		ErrorMsg:  resp.Header.Message,
	}
	if len(resp.Payload.Choices.Text) > 0 {
		frame.Content = resp.Payload.Choices.Text[0].Content
	}
	return frame, nil
}
