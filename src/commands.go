package main

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

var queryAliases = map[string]struct{}{"/query": {}, "/查询": {}, "/本子": {}}
var downloadAliases = map[string]struct{}{"/pdf": {}, "/download": {}, "/dl": {}, "/下载": {}}

type ParsedCommand struct {
	Type string
	ID   string
}

func ParseCommand(text string) (ParsedCommand, bool) {
	parts := strings.Fields(strings.TrimSpace(text))
	if len(parts) < 2 {
		return ParsedCommand{}, false
	}
	id := digitsOnly(parts[1])
	if id == "" {
		return ParsedCommand{}, false
	}
	if _, ok := queryAliases[parts[0]]; ok {
		return ParsedCommand{Type: "query", ID: id}, true
	}
	if _, ok := downloadAliases[parts[0]]; ok {
		return ParsedCommand{Type: "download", ID: id}, true
	}
	return ParsedCommand{}, false
}

func IsNonHelpCommandHeader(text string) bool {
	parts := strings.Fields(strings.TrimSpace(text))
	if len(parts) == 0 {
		return false
	}
	_, query := queryAliases[parts[0]]
	_, download := downloadAliases[parts[0]]
	return query || download
}

func IsHelpCommand(text string) bool {
	parts := strings.Fields(strings.TrimSpace(text))
	if len(parts) == 0 {
		return false
	}
	return parts[0] == "/help" || parts[0] == "/帮助" || parts[0] == "/?"
}

func BuildHelpMessage() string {
	return strings.Join([]string{
		"指令　　　功能　　　　别名",
		"────────────────────────────",
		"/query  查询本子信息  /查询, /本子",
		"/pdf    下载本子PDF   /download, /dl",
		"/help   帮助信息     /帮助, /?",
	}, "\n")
}

func ExtractCommandText(context MessageContext) (string, bool) {
	if context.MessageType != "group" && context.MessageType != "private" {
		return "", false
	}
	raw := extractText(context.Segments)
	if context.MessageType == "group" {
		if isBotMentioned(context) {
			return raw, true
		}
		return raw, IsNonHelpCommandHeader(raw)
	}
	if !strings.HasPrefix(raw, "/") {
		return "", false
	}
	return raw, true
}

func isBotMentioned(context MessageContext) bool {
	if context.MessageType != "group" {
		return false
	}
	selfID := strconv.FormatInt(context.SelfID, 10)
	for _, segment := range context.Segments {
		if segment.Type == "at" && segmentValueString(segment.Data, "qq") == selfID {
			return true
		}
	}
	return false
}

func extractText(segments []MessageSegment) string {
	var builder strings.Builder
	for _, segment := range segments {
		if segment.Type == "text" {
			if value, ok := segment.Data["text"].(string); ok && value != "" {
				builder.WriteString(value)
			}
		}
	}
	return strings.TrimSpace(builder.String())
}

func segmentValueString(data map[string]any, key string) string {
	value, ok := data[key]
	if !ok {
		return ""
	}
	switch value := value.(type) {
	case string:
		return value
	case float64:
		return strconv.FormatInt(int64(value), 10)
	case int64:
		return strconv.FormatInt(value, 10)
	case int:
		return strconv.Itoa(value)
	default:
		return fmt.Sprint(value)
	}
}

func digitsOnly(value string) string {
	var builder strings.Builder
	for _, char := range value {
		if char >= '0' && char <= '9' {
			builder.WriteRune(char)
		}
	}
	return builder.String()
}

type RateLimiter struct {
	mu      sync.Mutex
	buckets map[int64][]time.Time
	window  time.Duration
	max     int
}

func NewRateLimiter(cfg Config) *RateLimiter {
	return &RateLimiter{buckets: make(map[int64][]time.Time), window: time.Duration(cfg.RateLimitWindowMS) * time.Millisecond, max: cfg.RateLimitMaxRequests}
}

func (r *RateLimiter) Try(userID int64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	timestamps := r.buckets[userID]
	valid := timestamps[:0]
	for _, timestamp := range timestamps {
		if now.Sub(timestamp) < r.window {
			valid = append(valid, timestamp)
		}
	}
	if len(valid) >= r.max {
		r.buckets[userID] = valid
		return false
	}
	valid = append(valid, now)
	r.buckets[userID] = valid
	return true
}

type Bot struct {
	service *Service
	api     NapcatAPI
	cfg     Config
	limit   *RateLimiter

	mu       sync.RWMutex
	selfID   int64
	nickname string
}

func NewBot(service *Service, api NapcatAPI, cfg Config) *Bot {
	return &Bot{service: service, api: api, cfg: cfg, limit: NewRateLimiter(cfg), nickname: "JMComic Bot"}
}

func (b *Bot) SetLoginInfo(info LoginInfo) {
	b.mu.Lock()
	b.selfID = info.UserID
	b.nickname = info.Nickname
	b.mu.Unlock()
	appLog.Info("bot logged in", "user_id", info.UserID, "nickname", info.Nickname)
}

func (b *Bot) HandleFriendRequest(userID int64, comment string) {
	appLog.Info("friend request handled", "user_id", userID, "comment", shortText(comment, 160))
}

func (b *Bot) HandleMessage(message MessageContext) {
	text, ok := ExtractCommandText(message)
	if !ok {
		appLog.Debug("message ignored", "message_type", message.MessageType, "user_id", message.UserID, "group_id", message.GroupID, "text", shortText(extractText(message.Segments), 160))
		return
	}
	appLog.Info("command received", "message_type", message.MessageType, "user_id", message.UserID, "group_id", message.GroupID, "text", shortText(text, 160))
	if !b.limit.Try(message.UserID) {
		appLog.Warn("command rate limited", "user_id", message.UserID, "text", shortText(text, 160))
		_ = b.reply(context.Background(), message, BuildNotificationMessage("操作过于频繁，请稍后再试", message.UserID))
		return
	}
	if IsHelpCommand(text) {
		_ = b.reply(context.Background(), message, BuildNotificationMessage("\n"+BuildHelpMessage(), message.UserID))
		return
	}
	command, ok := ParseCommand(text)
	if !ok {
		appLog.Warn("command could not be parsed", "text", shortText(text, 160))
		_ = b.reply(context.Background(), message, BuildNotificationMessage("\n"+BuildHelpMessage(), message.UserID))
		return
	}
	appLog.Info("command parsed", "type", command.Type, "id", command.ID, "user_id", message.UserID, "group_id", message.GroupID)
	if command.Type == "query" {
		b.handleQuery(message, command.ID)
	} else {
		b.handleDownload(message, command.ID)
	}
}

func (b *Bot) handleQuery(message MessageContext, id string) {
	started := time.Now()
	appLog.Info("query started", "id", id, "user_id", message.UserID, "group_id", message.GroupID)
	if blockedID(id) {
		appLog.Warn("query blocked by rule", "id", id, "user_id", message.UserID)
		_ = b.reply(context.Background(), message, blockedReply(message.UserID))
		return
	}
	cached := b.service.IsInfoCached(id)
	appLog.Debug("query cache checked", "id", id, "hit", cached)
	if !cached {
		_ = b.reply(context.Background(), message, BuildNotificationMessage("查询中，请稍候...", message.UserID))
	}
	info, err := b.service.QueryInfo(context.Background(), id)
	if err != nil {
		appLog.Error("query failed", "id", id, "elapsed", time.Since(started), "error", err)
		_ = b.reply(context.Background(), message, BuildNotificationMessage("查询失败："+extractErrorMessage(err), message.UserID))
		return
	}
	appLog.Info("query data ready", "id", id, "elapsed", time.Since(started), "cover", info.Cover != nil)
	text := buildInfoText(info) + "\n\n阅读：https://j.2kb.fish/reader/" + id
	if b.trySendInfoForward(message, text, info.Cover) {
		if message.MessageType != "private" {
			_ = b.reply(context.Background(), message, []MessageSegment{AtSegment(message.UserID)})
		}
		appLog.Info("query completed", "id", id, "elapsed", time.Since(started), "delivery", "forward")
		return
	}
	_ = b.reply(context.Background(), message, BuildNotificationMessage(text, message.UserID))
	appLog.Info("query completed", "id", id, "elapsed", time.Since(started), "delivery", "plain")
}

func (b *Bot) trySendInfoForward(message MessageContext, text string, cover *string) bool {
	b.mu.RLock()
	selfID, nickname := b.selfID, b.nickname
	b.mu.RUnlock()
	textNode := ForwardNode{Content: []MessageSegment{TextSegment(text)}, UserID: strconv.FormatInt(selfID, 10), Nickname: nickname}
	nodes := []ForwardNode{textNode}
	if cover != nil {
		nodes = append(nodes, ForwardNode{Content: []MessageSegment{ImageSegment(*cover)}, UserID: strconv.FormatInt(selfID, 10), Nickname: nickname, Summary: "[封面]"})
	}
	appLog.Info("sending info forward", "message_type", message.MessageType, "user_id", message.UserID, "group_id", message.GroupID, "nodes", len(nodes))
	if err := b.api.SendForward(context.Background(), message, nodes); err == nil {
		return true
	} else {
		appLog.Warn("info forward failed; trying text-only", "error", err)
	}
	if err := b.api.SendForward(context.Background(), message, []ForwardNode{textNode}); err != nil {
		appLog.Warn("text-only forward failed; falling back to plain message", "error", err)
		return false
	}
	return true
}

func (b *Bot) handleDownload(message MessageContext, id string) {
	started := time.Now()
	appLog.Info("PDF request started", "id", id, "user_id", message.UserID, "group_id", message.GroupID)
	if blockedID(id) {
		appLog.Warn("PDF request blocked by rule", "id", id, "user_id", message.UserID)
		_ = b.reply(context.Background(), message, blockedReply(message.UserID))
		return
	}
	_ = b.reply(context.Background(), message, BuildNotificationMessage("下载中", message.UserID))
	enqueued := b.service.EnqueuePDF(id)
	appLog.Info("PDF task enqueue result", "id", id, "status", enqueued.Status, "error", enqueued.Error)
	if enqueued.Status == "error" {
		_ = b.reply(context.Background(), message, BuildNotificationMessage("操作失败："+enqueued.Error, message.UserID))
		return
	}
	sentEstimate := false
	lastStatus := ""
	for attempt := 0; attempt < b.cfg.MaxPollAttempts; attempt++ {
		time.Sleep(time.Duration(b.cfg.PollIntervalMS) * time.Millisecond)
		status := b.service.PDFStatus(id)
		if status.Status != lastStatus {
			appLog.Info("PDF task status changed", "id", id, "status", status.Status, "attempt", attempt+1)
			lastStatus = status.Status
		}
		if status.Status == "ready" {
			appLog.Info("PDF ready; encrypting", "id", id, "elapsed", time.Since(started))
			data, err := b.service.ReadPDF(id)
			if err != nil {
				_ = b.reply(context.Background(), message, BuildNotificationMessage("PDF 生成失败："+extractErrorMessage(err), message.UserID))
				return
			}
			encryptionStarted := time.Now()
			encrypted, err := EncryptPDF(data, id)
			if err != nil {
				appLog.Error("PDF encryption failed", "id", id, "elapsed", time.Since(encryptionStarted), "error", err)
				_ = b.reply(context.Background(), message, BuildNotificationMessage("PDF 生成失败："+extractErrorMessage(err), message.UserID))
				return
			}
			appLog.Info("PDF encrypted", "id", id, "input_bytes", len(data), "output_bytes", len(encrypted), "elapsed", time.Since(encryptionStarted))
			if !b.trySendPDFForward(message, encrypted, id) {
				_ = b.reply(context.Background(), message, BuildFileNotification(encrypted, id+".pdf", message.UserID))
			}
			appLog.Info("PDF request completed", "id", id, "elapsed", time.Since(started))
			return
		}
		if status.Status == "error" {
			appLog.Error("PDF task failed", "id", id, "elapsed", time.Since(started), "error", status.Error)
			_ = b.reply(context.Background(), message, BuildNotificationMessage("PDF 生成失败："+status.Error, message.UserID))
			return
		}
		if status.Status == "processing" && status.Progress != nil && status.Progress.ETAPSeconds != nil && !sentEstimate {
			_ = b.reply(context.Background(), message, BuildNotificationMessage(fmt.Sprintf("预计还需要 %d 秒", *status.Progress.ETAPSeconds), message.UserID))
			sentEstimate = true
		}
	}
	appLog.Warn("PDF request timed out", "id", id, "elapsed", time.Since(started), "max_attempts", b.cfg.MaxPollAttempts)
	_ = b.reply(context.Background(), message, BuildNotificationMessage("PDF 生成超时，请稍后重试", message.UserID))
}

func (b *Bot) trySendPDFForward(message MessageContext, data []byte, id string) bool {
	b.mu.RLock()
	selfID, nickname := b.selfID, b.nickname
	b.mu.RUnlock()
	nodes := []ForwardNode{
		{Content: []MessageSegment{TextSegment("密码：" + id)}, UserID: strconv.FormatInt(selfID, 10), Nickname: nickname},
		{Content: []MessageSegment{FileSegment(data, id+".pdf")}, UserID: strconv.FormatInt(selfID, 10), Nickname: nickname},
	}
	if err := b.api.SendForward(context.Background(), message, nodes); err != nil {
		appLog.Warn("PDF forward failed; falling back to direct file", "id", id, "error", err)
		return false
	}
	return true
}

func (b *Bot) reply(ctx context.Context, target MessageContext, message []MessageSegment) error {
	if target.MessageType == "private" {
		filtered := make([]MessageSegment, 0, len(message))
		for _, segment := range message {
			if segment.Type != "at" {
				filtered = append(filtered, segment)
			}
		}
		message = filtered
	}
	err := b.api.Send(ctx, target, message)
	if err != nil {
		appLog.Warn("reply failed", "message_type", target.MessageType, "user_id", target.UserID, "group_id", target.GroupID, "segments", len(message), "error", err)
	}
	return err
}

func BuildNotificationMessage(text string, userID int64) []MessageSegment {
	segments := make([]MessageSegment, 0, 2)
	if userID != 0 {
		segments = append(segments, AtSegment(userID))
	}
	return append(segments, TextSegment("\n"+text))
}

func BuildFileNotification(data []byte, name string, userID int64) []MessageSegment {
	segments := make([]MessageSegment, 0, 2)
	if userID != 0 {
		segments = append(segments, AtSegment(userID))
	}
	return append(segments, FileSegment(data, name))
}

func blockedReply(userID int64) []MessageSegment {
	return []MessageSegment{AtSegment(userID), TextSegment("\n这么喜欢董卓奖励你和董卓做呱😡😡😡")}
}

func blockedID(id string) bool { return id == "350234" || id == "350235" }

func buildInfoText(info InfoResponse) string {
	lines := make([]string, 0, 9)
	appendIf := func(value string, ok bool) {
		if ok {
			lines = append(lines, value)
		}
	}
	appendIf("名称："+info.Name, true)
	if info.Description != nil {
		appendIf("简介："+*info.Description, *info.Description != "")
	}
	if len(info.Authors) > 0 {
		appendIf("作者："+strings.Join(info.Authors, ", "), true)
	}
	if len(info.Tags) > 0 {
		appendIf("标签："+strings.Join(info.Tags, ", "), true)
	}
	if len(info.Works) > 0 {
		appendIf("作品："+strings.Join(info.Works, ", "), true)
	}
	if len(info.Actors) > 0 {
		appendIf("演员："+strings.Join(info.Actors, ", "), true)
	}
	if info.Views != nil && *info.Views != "" {
		appendIf("浏览："+*info.Views, true)
	}
	if info.Likes != nil && *info.Likes != "" {
		appendIf("点赞："+*info.Likes, true)
	}
	if info.Views != nil && info.Likes != nil {
		views, viewsErr := strconv.ParseFloat(*info.Views, 64)
		likes, likesErr := strconv.ParseFloat(*info.Likes, 64)
		if viewsErr == nil && views > 0 {
			if likesErr == nil {
				appendIf(fmt.Sprintf("点赞率：%.1f%%", likes/views*100), true)
			} else {
				appendIf("点赞率：NaN%", true)
			}
		}
	}
	return strings.Join(lines, "\n\n")
}
