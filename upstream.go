package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type PhotoInfo struct {
	ScrambleID int         `json:"scrambleId"`
	Name       string      `json:"name"`
	ID         string      `json:"id"`
	Images     []ImageInfo `json:"images"`
}

type ImageInfo struct {
	Name string `json:"name"`
	URL  string `json:"url"`
}

type AlbumInfo struct {
	ID          string   `json:"id"`
	Description string   `json:"description"`
	TotalViews  string   `json:"totalViews"`
	Likes       string   `json:"likes"`
	Authors     []string `json:"author"`
	Tags        []string `json:"tags"`
	Works       []string `json:"works"`
	Actors      []string `json:"actors"`
}

type UpstreamErrorKind string

const (
	UpstreamInvalid  UpstreamErrorKind = "upstream_response_invalid"
	UpstreamNotFound UpstreamErrorKind = "not_found"
	UpstreamUnknown  UpstreamErrorKind = "unknown_error"
)

type UpstreamError struct {
	Kind   UpstreamErrorKind
	Status int
	Body   string
}

func (e *UpstreamError) Error() string {
	if e.Body != "" {
		return fmt.Sprintf("%s (%d): %s", e.Kind, e.Status, e.Body)
	}
	return fmt.Sprintf("%s (%d)", e.Kind, e.Status)
}

type UpstreamClient struct {
	baseURL string
	client  *http.Client
	timeout time.Duration
}

func NewUpstreamClient(cfg Config) *UpstreamClient {
	return &UpstreamClient{
		baseURL: strings.TrimRight(cfg.UpstreamBaseURL, "/"),
		client:  &http.Client{},
		timeout: time.Duration(cfg.UpstreamTimeoutMs) * time.Millisecond,
	}
}

func (c *UpstreamClient) QueryPhoto(ctx context.Context, id string) (PhotoInfo, error) {
	var result PhotoInfo
	err := c.getJSON(ctx, "/photo/"+url.PathEscape(id), &result)
	return result, err
}

func (c *UpstreamClient) QueryAlbum(ctx context.Context, id string) (AlbumInfo, error) {
	var result AlbumInfo
	err := c.getJSON(ctx, "/album/"+url.PathEscape(id), &result)
	return result, err
}

func (c *UpstreamClient) getJSON(ctx context.Context, path string, target any) error {
	requestCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	request, err := http.NewRequestWithContext(requestCtx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return err
	}
	response, err := c.client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNotFound {
		return &UpstreamError{Kind: UpstreamNotFound, Status: response.StatusCode}
	}
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 2048))
		return &UpstreamError{Kind: UpstreamUnknown, Status: response.StatusCode, Body: string(body)}
	}
	decoder := json.NewDecoder(response.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return &UpstreamError{Kind: UpstreamInvalid, Status: response.StatusCode}
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return &UpstreamError{Kind: UpstreamInvalid, Status: response.StatusCode}
	}
	return nil
}

func parsePhotoID(id string) int {
	value, err := strconv.Atoi(id)
	if err != nil || value < 0 {
		return 0
	}
	return value
}
