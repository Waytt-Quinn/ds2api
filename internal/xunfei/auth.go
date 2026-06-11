// Package xunfei implements a WebSocket-based backend that talks to
// the Xunfei/Spark WSS protocol used by the internal ai-agent
// gateway (wss://ai-agent/agent/skybox/api/v1/chat). The xunfei
// protocol is fundamentally different from the HTTP/SSE protocol
// ds2api was built for, so this package owns its own transport,
// payload format, and stream frame parsing.
//
// What is REUSED (not duplicated) is the DSML prompt injection, the
// tool-call parser, the empty-output retry, the assistantturn
// event model, and the OpenAI/Anthropic protocol translation —
// those all live in higher-level ds2api packages and see no
// knowledge of the upstream transport.
package xunfei

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"net/url"
	"strings"
	"time"
)

// BuildAuthURL signs a Xunfei WSS URL using HMAC-SHA256 over the
// canonical "host\ndate\nrequest-line" string. Returns the domain
// (from the Xunfei model version, e.g. "generalv3.5" for v3.5) and
// the WSS URL with host/date/authorization query parameters.
//
// Ported from D:\Workspace\one-api\relay\adaptor\xunfei/main.go
// buildXunfeiAuthUrl.
func BuildAuthURL(host, path, apiKey, apiSecret string) (domain, authURL string) {
	h := hmacWithShaToBase64("hmac-sha256",
		"host: "+host+"\n"+"date: "+time.Now().UTC().Format(time.RFC1123)+"\n"+"GET "+path+" HTTP/1.1",
		apiSecret)
	authString := fmt.Sprintf(
		`hmac username="%s", algorithm="%s", headers="%s", signature="%s"`,
		apiKey, "hmac-sha256", "host date request-line", h)
	authorization := base64.StdEncoding.EncodeToString([]byte(authString))

	u, err := url.Parse("wss://" + host + path)
	if err != nil {
		return "", ""
	}

	q := u.Query()
	q.Add("host", host)
	q.Add("date", time.Now().UTC().Format(time.RFC1123))
	q.Add("authorization", authorization)
	u.RawQuery = q.Encode()
	return "", u.String()
}

func hmacWithShaToBase64(algorithm, data, key string) string {
	mac := hmac.New(sha256.New, []byte(key))
	mac.Write([]byte(data))
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

// DomainForAPIVersion maps an OpenAI-style "model version" string
// (e.g. "v3.5", "4.0Ultra") to the Xunfei "domain" field. The
// ai-agent gateway typically overrides this via the XUNFEI_DOMAIN
// env var, in which case this mapper is bypassed.
//
// Ported from D:\Workspace\one-api\relay\adaptor\xunfei/main.go
// apiVersion2domain.
func DomainForAPIVersion(apiVersion string) string {
	switch strings.TrimSpace(apiVersion) {
	case "v1.1":
		return "lite"
	case "v2.1":
		return "generalv2"
	case "v3.1":
		return "generalv3"
	case "v3.1-128K":
		return "pro-128k"
	case "v3.5":
		return "generalv3.5"
	case "v3.5-32K":
		return "max-32k"
	case "v4.0":
		return "4.0Ultra"
	}
	return "general" + strings.TrimSpace(apiVersion)
}
