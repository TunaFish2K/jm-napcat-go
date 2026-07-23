package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestUpstreamClientParsesPhotoAndAlbum(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case "/photo/123":
			_, _ = writer.Write([]byte(`{"scrambleId":1,"name":"demo","id":"123","images":[{"name":"1.jpg","url":"https://example.test/1.jpg"}]}`))
		case "/album/123":
			_, _ = writer.Write([]byte(`{"id":"123","description":"desc","totalViews":"10","likes":"2","author":["a"],"tags":[],"works":[],"actors":[]}`))
		default:
			writer.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	cfg := DefaultConfig()
	cfg.UpstreamBaseURL = server.URL
	client := NewUpstreamClient(cfg)
	photo, err := client.QueryPhoto(context.Background(), "123")
	if err != nil || photo.Name != "demo" || len(photo.Images) != 1 {
		t.Fatalf("photo = %#v, err=%v", photo, err)
	}
	album, err := client.QueryAlbum(context.Background(), "123")
	if err != nil || album.Description != "desc" {
		t.Fatalf("album = %#v, err=%v", album, err)
	}
}

func TestUpstreamClientRejectsUnknownFields(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		_, _ = writer.Write([]byte(`{"scrambleId":1,"name":"demo","id":"123","images":[],"extra":true}`))
	}))
	defer server.Close()
	cfg := DefaultConfig()
	cfg.UpstreamBaseURL = server.URL
	_, err := NewUpstreamClient(cfg).QueryPhoto(context.Background(), "123")
	if err == nil {
		t.Fatal("unknown upstream field was accepted")
	}
}
