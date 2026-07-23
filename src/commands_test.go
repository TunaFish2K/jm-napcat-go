package main

import (
	"testing"
)

func TestParseCommandAndMessageFilter(t *testing.T) {
	command, ok := ParseCommand("/查询 JM-12abc")
	if !ok || command.Type != "query" || command.ID != "12" {
		t.Fatalf("ParseCommand() = %#v, %v", command, ok)
	}
	group := MessageContext{MessageType: "group", SelfID: 99, UserID: 7, Segments: []MessageSegment{
		{Type: "at", Data: map[string]any{"qq": "99"}},
		TextSegment(" /pdf 123 "),
	}}
	if text, ok := ExtractCommandText(group); !ok || text != "/pdf 123" {
		t.Fatalf("mentioned group text = %q, %v", text, ok)
	}
	unmentioned := MessageContext{MessageType: "group", SelfID: 99, UserID: 7, Segments: []MessageSegment{TextSegment("/pdf 123")}}
	if _, ok := ExtractCommandText(unmentioned); !ok {
		t.Fatal("unmentioned valid command was ignored")
	}
	ignored := MessageContext{MessageType: "group", SelfID: 99, UserID: 7, Segments: []MessageSegment{TextSegment("hello")}}
	if _, ok := ExtractCommandText(ignored); ok {
		t.Fatal("unrelated group message was accepted")
	}
	private := MessageContext{MessageType: "private", UserID: 7, Segments: []MessageSegment{TextSegment("query 123")}}
	if _, ok := ExtractCommandText(private); ok {
		t.Fatal("private non-slash message was accepted")
	}
}

func TestRateLimiter(t *testing.T) {
	cfg := DefaultConfig()
	limiter := NewRateLimiter(cfg)
	for i := 0; i < cfg.RateLimitMaxRequests; i++ {
		if !limiter.Try(1) {
			t.Fatalf("request %d was rejected", i)
		}
	}
	if limiter.Try(1) {
		t.Fatal("request over rate limit was accepted")
	}
}

func TestBuildInfoText(t *testing.T) {
	views, likes := "100", "3"
	text := buildInfoText(InfoResponse{Name: "name", Views: &views, Likes: &likes, Authors: []string{"a", "b"}})
	want := "名称：name\n\n作者：a, b\n\n浏览：100\n\n点赞：3\n\n点赞率：3.0%"
	if text != want {
		t.Fatalf("buildInfoText() = %q, want %q", text, want)
	}
}
