package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestDecodeMessageEvent(t *testing.T) {
	data := []byte(`{"post_type":"message","message_type":"group","self_id":99,"user_id":7,"group_id":8,"message":[{"type":"at","data":{"qq":"99"}},{"type":"text","data":{"text":" /help "}}]}`)
	message, err := decodeMessageEvent(data)
	if err != nil {
		t.Fatal(err)
	}
	if message.MessageType != "group" || message.SelfID != 99 || len(message.Segments) != 2 {
		t.Fatalf("decoded message = %#v", message)
	}
	if !isBotMentioned(message) {
		t.Fatal("decoded @ segment was not recognized")
	}
}

func TestOutboundSegmentsUseNapcatShape(t *testing.T) {
	data, err := json.Marshal([]MessageSegment{AtSegment(42), TextSegment("hello"), FileSegment([]byte("pdf"), "x.pdf")})
	if err != nil {
		t.Fatal(err)
	}
	var segments []MessageSegment
	if err := json.Unmarshal(data, &segments); err != nil {
		t.Fatal(err)
	}
	if len(segments) != 3 || segments[0].Type != "at" || segments[1].Data["text"] != "hello" || segments[2].Data["name"] != "x.pdf" {
		t.Fatalf("segments = %#v", segments)
	}
}

func TestNapcatForwardTransportMapsActions(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	actions := make(chan map[string]any, 8)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := upgrader.Upgrade(writer, request, nil)
		if err != nil {
			return
		}
		defer connection.Close()
		for {
			var action map[string]any
			if err := connection.ReadJSON(&action); err != nil {
				return
			}
			actions <- action
			response := map[string]any{"status": "ok", "retcode": 0, "echo": action["echo"]}
			if action["action"] == "get_login_info" {
				response["data"] = map[string]any{"user_id": 123, "nickname": "bot"}
			}
			if err := connection.WriteJSON(response); err != nil {
				return
			}
		}
	}))
	defer server.Close()

	cfg := DefaultConfig()
	cfg.NapcatWSURL = "ws" + strings.TrimPrefix(server.URL, "http")
	client := NewNapcatClient(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	login := make(chan LoginInfo, 1)
	runErr := make(chan error, 1)
	go func() { runErr <- client.Run(ctx, nil, nil, func(info LoginInfo) { login <- info }) }()
	select {
	case info := <-login:
		if info.UserID != 123 || info.Nickname != "bot" {
			t.Fatalf("login = %#v", info)
		}
	case <-time.After(time.Second):
		t.Fatal("login response timed out")
	}
	<-actions
	target := MessageContext{MessageType: "group", GroupID: 456}
	if err := client.Send(ctx, target, []MessageSegment{TextSegment("hello")}); err != nil {
		t.Fatal(err)
	}
	sendAction := <-actions
	if sendAction["action"] != "send_msg" {
		t.Fatalf("send action = %#v", sendAction["action"])
	}
	params := sendAction["params"].(map[string]any)
	if params["group_id"].(float64) != 456 {
		t.Fatalf("send params = %#v", params)
	}
	if err := client.SendForward(ctx, target, []ForwardNode{{UserID: "123", Nickname: "bot", Content: []MessageSegment{TextSegment("hello")}}}); err != nil {
		t.Fatal(err)
	}
	forwardAction := <-actions
	if forwardAction["action"] != "send_forward_msg" {
		t.Fatalf("forward action = %#v", forwardAction["action"])
	}
	cancel()
	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("Run() = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("client did not stop")
	}
}
