package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/q1bksuu/onebot-go-sdk/v11/entity"
)

// The SDK exposes typed OneBot entities, but its client transport is a
// reverse/Universal client. NapCat here is a forward WebSocket server, so the
// small transport below reuses the SDK's typed login response and keeps all
// forward protocol handling in one adapter.
type MessageSegment struct {
	Type string         `json:"type"`
	Data map[string]any `json:"data"`
}

type MessageContext struct {
	MessageType string
	SelfID      int64
	UserID      int64
	GroupID     int64
	Segments    []MessageSegment
}

type ForwardNode struct {
	Content  []MessageSegment
	UserID   string
	Nickname string
	Summary  string
}

type LoginInfo struct {
	UserID   int64
	Nickname string
}

type NapcatAPI interface {
	Send(ctx context.Context, target MessageContext, message []MessageSegment) error
	SendForward(ctx context.Context, target MessageContext, nodes []ForwardNode) error
	LoginInfo(ctx context.Context) (LoginInfo, error)
}

type actionResponse struct {
	Status  string          `json:"status"`
	Retcode int             `json:"retcode"`
	Data    json.RawMessage `json:"data"`
	Message string          `json:"message"`
	Echo    json.RawMessage `json:"echo"`
}

type NapcatClient struct {
	cfg Config

	connMu sync.RWMutex
	conn   *websocket.Conn

	writeMu   sync.Mutex
	pendingMu sync.Mutex
	pending   map[string]chan actionResponse
	nextEcho  atomic.Uint64
}

func NewNapcatClient(cfg Config) *NapcatClient {
	return &NapcatClient{cfg: cfg, pending: make(map[string]chan actionResponse)}
}

func (c *NapcatClient) Run(ctx context.Context, onMessage func(MessageContext), onFriendRequest func(int64, string), onLogin func(LoginInfo)) error {
	const reconnectAttempts = 10
	var lastErr error
	for attempt := 0; attempt <= reconnectAttempts; attempt++ {
		if ctx.Err() != nil {
			return nil
		}
		conn, err := c.dial(ctx)
		if err == nil {
			err = c.runConnection(ctx, conn, onMessage, onFriendRequest, onLogin)
		}
		if ctx.Err() != nil {
			return nil
		}
		lastErr = err
		if attempt < reconnectAttempts {
			fmt.Printf("Napcat connection failed (%d/%d): %v; retrying in 5s\n", attempt+1, reconnectAttempts, err)
			timer := time.NewTimer(5 * time.Second)
			select {
			case <-timer.C:
			case <-ctx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				return nil
			}
		}
	}
	return fmt.Errorf("Napcat connection failed after retries: %w", lastErr)
}

func (c *NapcatClient) dial(ctx context.Context) (*websocket.Conn, error) {
	requestHeader := make(http.Header)
	if c.cfg.NapcatAccessToken != "" {
		requestHeader.Set("Authorization", "Bearer "+c.cfg.NapcatAccessToken)
	}
	dialer := websocket.Dialer{HandshakeTimeout: 10 * time.Second}
	conn, response, err := dialer.DialContext(ctx, c.cfg.NapcatWSURL, requestHeader)
	if response != nil && response.Body != nil {
		_ = response.Body.Close()
	}
	return conn, err
}

func (c *NapcatClient) runConnection(ctx context.Context, conn *websocket.Conn, onMessage func(MessageContext), onFriendRequest func(int64, string), onLogin func(LoginInfo)) error {
	c.connMu.Lock()
	c.conn = conn
	c.connMu.Unlock()
	defer func() {
		c.connMu.Lock()
		if c.conn == conn {
			c.conn = nil
		}
		c.connMu.Unlock()
		c.failPending(errors.New("Napcat connection closed"))
		_ = conn.Close()
	}()

	readErrors := make(chan error, 1)
	go func() { readErrors <- c.readLoop(ctx, conn, onMessage, onFriendRequest) }()
	login, err := c.LoginInfo(ctx)
	if err != nil {
		_ = conn.Close()
		return err
	}
	if onLogin != nil {
		onLogin(login)
	}
	fmt.Printf("Connected to Napcat\n")

	select {
	case <-ctx.Done():
		_ = conn.Close()
		return nil
	case err := <-readErrors:
		return err
	}
}

func (c *NapcatClient) readLoop(ctx context.Context, conn *websocket.Conn, onMessage func(MessageContext), onFriendRequest func(int64, string)) error {
	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			return err
		}
		var envelope struct {
			Echo     json.RawMessage `json:"echo"`
			PostType string          `json:"post_type"`
		}
		if err := json.Unmarshal(data, &envelope); err != nil {
			continue
		}
		if len(envelope.Echo) > 0 && string(envelope.Echo) != "null" {
			var response actionResponse
			if json.Unmarshal(data, &response) == nil {
				c.resolvePending(echoString(envelope.Echo), response)
			}
			continue
		}
		if envelope.PostType == "message" {
			message, err := decodeMessageEvent(data)
			if err == nil && onMessage != nil {
				onMessage(message)
			}
			continue
		}
		if envelope.PostType == "request" {
			var request struct {
				RequestType string `json:"request_type"`
				UserID      int64  `json:"user_id"`
				Comment     string `json:"comment"`
			}
			if json.Unmarshal(data, &request) == nil && request.RequestType == "friend" && onFriendRequest != nil {
				onFriendRequest(request.UserID, request.Comment)
			}
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}

func decodeMessageEvent(data []byte) (MessageContext, error) {
	var event struct {
		MessageType string          `json:"message_type"`
		SelfID      int64           `json:"self_id"`
		UserID      int64           `json:"user_id"`
		GroupID     int64           `json:"group_id"`
		Message     json.RawMessage `json:"message"`
	}
	if err := json.Unmarshal(data, &event); err != nil {
		return MessageContext{}, err
	}
	var segments []MessageSegment
	if len(event.Message) > 0 && event.Message[0] == '[' {
		if err := json.Unmarshal(event.Message, &segments); err != nil {
			return MessageContext{}, err
		}
	} else if len(event.Message) > 0 && event.Message[0] == '"' {
		var text string
		if err := json.Unmarshal(event.Message, &text); err == nil {
			segments = []MessageSegment{TextSegment(text)}
		}
	}
	return MessageContext{MessageType: event.MessageType, SelfID: event.SelfID, UserID: event.UserID, GroupID: event.GroupID, Segments: segments}, nil
}

func (c *NapcatClient) LoginInfo(ctx context.Context) (LoginInfo, error) {
	response, err := c.call(ctx, "get_login_info", map[string]any{})
	if err != nil {
		return LoginInfo{}, err
	}
	var login entity.GetLoginInfoResponse
	if err := json.Unmarshal(response.Data, &login); err != nil {
		return LoginInfo{}, err
	}
	return LoginInfo{UserID: login.UserId, Nickname: login.Nickname}, nil
}

func (c *NapcatClient) Send(ctx context.Context, target MessageContext, message []MessageSegment) error {
	params := map[string]any{"message": message}
	if target.MessageType == "private" {
		params["user_id"] = target.UserID
	} else {
		params["group_id"] = target.GroupID
	}
	_, err := c.call(ctx, "send_msg", params)
	return err
}

func (c *NapcatClient) SendForward(ctx context.Context, target MessageContext, nodes []ForwardNode) error {
	serialized := make([]MessageSegment, 0, len(nodes))
	for _, node := range nodes {
		data := map[string]any{
			"user_id":  node.UserID,
			"nickname": node.Nickname,
			"content":  node.Content,
		}
		if node.Summary != "" {
			data["summary"] = node.Summary
		}
		serialized = append(serialized, MessageSegment{Type: "node", Data: data})
	}
	params := map[string]any{"message": serialized}
	if target.MessageType == "private" {
		params["user_id"] = target.UserID
		_, err := c.call(ctx, "send_private_forward_msg", params)
		return err
	}
	params["group_id"] = target.GroupID
	_, err := c.call(ctx, "send_forward_msg", params)
	return err
}

func (c *NapcatClient) call(ctx context.Context, action string, params map[string]any) (actionResponse, error) {
	c.connMu.RLock()
	conn := c.conn
	c.connMu.RUnlock()
	if conn == nil {
		return actionResponse{}, errors.New("Napcat instance not initialized")
	}
	echo := fmt.Sprintf("jm-%d", c.nextEcho.Add(1))
	responseChannel := make(chan actionResponse, 1)
	c.pendingMu.Lock()
	c.pending[echo] = responseChannel
	c.pendingMu.Unlock()
	request := map[string]any{"action": action, "params": params, "echo": echo}
	c.writeMu.Lock()
	writeErr := conn.WriteJSON(request)
	c.writeMu.Unlock()
	if writeErr != nil {
		c.pendingMu.Lock()
		delete(c.pending, echo)
		c.pendingMu.Unlock()
		return actionResponse{}, writeErr
	}
	select {
	case response := <-responseChannel:
		if response.Retcode != 0 || response.Status == "failed" {
			return response, fmt.Errorf("Napcat action %s failed (%d): %s", action, response.Retcode, response.Message)
		}
		return response, nil
	case <-ctx.Done():
		c.pendingMu.Lock()
		delete(c.pending, echo)
		c.pendingMu.Unlock()
		return actionResponse{}, ctx.Err()
	}
}

func (c *NapcatClient) resolvePending(echo string, response actionResponse) {
	c.pendingMu.Lock()
	channel := c.pending[echo]
	delete(c.pending, echo)
	c.pendingMu.Unlock()
	if channel != nil {
		channel <- response
	}
}

func (c *NapcatClient) failPending(err error) {
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()
	for echo, channel := range c.pending {
		channel <- actionResponse{Status: "failed", Retcode: -1, Message: err.Error(), Echo: json.RawMessage(strconv.Quote(echo))}
		delete(c.pending, echo)
	}
}

func echoString(raw json.RawMessage) string {
	var value string
	if json.Unmarshal(raw, &value) == nil {
		return value
	}
	return string(raw)
}

func TextSegment(text string) MessageSegment {
	return MessageSegment{Type: "text", Data: map[string]any{"text": text}}
}

func AtSegment(userID int64) MessageSegment {
	return MessageSegment{Type: "at", Data: map[string]any{"qq": strconv.FormatInt(userID, 10)}}
}

func ImageSegment(dataURL string) MessageSegment {
	return MessageSegment{Type: "image", Data: map[string]any{"file": toBase64URI(dataURL), "name": "cover.jpg"}}
}

func FileSegment(data []byte, name string) MessageSegment {
	return MessageSegment{Type: "file", Data: map[string]any{"file": "base64://" + base64Encode(data), "name": name}}
}

func toBase64URI(dataURL string) string {
	const prefix = "data:image/jpeg;base64,"
	if len(dataURL) >= len(prefix) && dataURL[:len(prefix)] == prefix {
		return "base64://" + dataURL[len(prefix):]
	}
	return dataURL
}

func base64Encode(data []byte) string {
	return base64.StdEncoding.EncodeToString(data)
}
