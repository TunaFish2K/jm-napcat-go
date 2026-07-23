package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"
)

type InfoResponse struct {
	Name        string   `json:"name"`
	Description *string  `json:"description"`
	Views       *string  `json:"views"`
	Likes       *string  `json:"likes"`
	Authors     []string `json:"authors"`
	Tags        []string `json:"tags"`
	Works       []string `json:"works"`
	Actors      []string `json:"actors"`
	Cover       *string  `json:"cover"`
}

type TaskProgress struct {
	TotalImages     int   `json:"totalImages"`
	ProcessedImages int   `json:"processedImages"`
	StartedAt       int64 `json:"startedAt"`
	ETAPSeconds     *int  `json:"etaSeconds,omitempty"`
}

type TaskStatusResult struct {
	Status   string        `json:"status"`
	Progress *TaskProgress `json:"progress,omitempty"`
	Error    string        `json:"error,omitempty"`
}

type taskState struct {
	status          string
	error           string
	retryCount      int
	updatedAt       time.Time
	totalImages     int
	processedImages int
	startedAt       time.Time
	etaSeconds      int
}

type taskStore struct {
	mu       sync.Mutex
	states   map[string]taskState
	errorTTL time.Duration
}

func newTaskStore(errorTTL time.Duration) *taskStore {
	return &taskStore{states: make(map[string]taskState), errorTTL: errorTTL}
}

func (s *taskStore) get(id string) (taskState, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.removeExpiredLocked()
	state, ok := s.states[id]
	return state, ok
}

func (s *taskStore) set(id string, state taskState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.removeExpiredLocked()
	if len(s.states) >= 1000 {
		for key, value := range s.states {
			if value.status == "error" {
				delete(s.states, key)
				break
			}
		}
	}
	s.states[id] = state
}

func (s *taskStore) delete(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.states, id)
}

func (s *taskStore) removeExpiredLocked() {
	now := time.Now()
	for id, state := range s.states {
		if state.status == "error" && now.Sub(state.updatedAt) > s.errorTTL {
			delete(s.states, id)
		}
	}
}

type taskQueue struct {
	mu     sync.Mutex
	max    int
	items  []string
	ids    map[string]struct{}
	notify chan struct{}
	closed bool
}

func newTaskQueue(maxSize int) *taskQueue {
	return &taskQueue{max: maxSize, ids: make(map[string]struct{}), notify: make(chan struct{})}
}

func (q *taskQueue) push(id string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return false
	}
	if _, exists := q.ids[id]; exists {
		return true
	}
	if len(q.items) >= q.max {
		return false
	}
	q.items = append(q.items, id)
	q.ids[id] = struct{}{}
	close(q.notify)
	q.notify = make(chan struct{})
	return true
}

func (q *taskQueue) pop() (string, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items) == 0 {
		return "", false
	}
	id := q.items[0]
	q.items = q.items[1:]
	delete(q.ids, id)
	return id, true
}

func (q *taskQueue) wait(ctx context.Context) bool {
	for {
		q.mu.Lock()
		if len(q.items) > 0 {
			q.mu.Unlock()
			return true
		}
		if q.closed {
			q.mu.Unlock()
			return false
		}
		notify := q.notify
		q.mu.Unlock()
		select {
		case <-notify:
		case <-ctx.Done():
			return false
		}
	}
}

func (q *taskQueue) close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return
	}
	q.closed = true
	close(q.notify)
}

type Service struct {
	cfg       Config
	upstream  *UpstreamClient
	images    *ImageProcessor
	infoCache *InfoCache
	pdfCache  *PDFCache
	queue     *taskQueue
	states    *taskStore

	mu        sync.Mutex
	shutting  bool
	workerCtx context.Context
	cancel    context.CancelFunc
	workers   sync.WaitGroup
}

func NewService(cfg Config) (*Service, error) {
	infoDir, err := resolvePath(cfg.InfoCacheDir)
	if err != nil {
		return nil, err
	}
	pdfDir, err := resolvePath(cfg.PDFCacheDir)
	if err != nil {
		return nil, err
	}
	cfg.InfoCacheDir = infoDir
	cfg.PDFCacheDir = pdfDir
	return &Service{
		cfg:       cfg,
		upstream:  NewUpstreamClient(cfg),
		images:    NewImageProcessor(cfg),
		infoCache: NewInfoCache(infoDir),
		pdfCache:  NewPDFCache(pdfDir, cfg.PDFCacheMaxSize),
		queue:     newTaskQueue(cfg.MaxTaskQueued),
		states:    newTaskStore(time.Duration(cfg.ErrorTTLMS) * time.Millisecond),
	}, nil
}

func (s *Service) Init() error {
	if err := s.infoCache.Init(); err != nil {
		return fmt.Errorf("initialize info cache: %w", err)
	}
	if err := s.pdfCache.Init(); err != nil {
		return fmt.Errorf("initialize PDF cache: %w", err)
	}
	total, maxSize, count := s.pdfCache.Stats()
	fmt.Printf("Info cache initialized at %s\n", s.cfg.InfoCacheDir)
	fmt.Printf("PDF cache initialized: %d records, %.1fMB / %.1fGB\n", count, float64(total)/1024/1024, float64(maxSize)/1024/1024/1024)
	return nil
}

func (s *Service) StartWorkers(parent context.Context) {
	s.workerCtx, s.cancel = context.WithCancel(parent)
	for index := 0; index < s.cfg.WorkerPoolSize; index++ {
		s.workers.Add(1)
		go s.runWorker(index + 1)
	}
	fmt.Printf("Started %d PDF workers\n", s.cfg.WorkerPoolSize)
}

func (s *Service) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	if !s.shutting {
		s.shutting = true
		if s.cancel != nil {
			s.cancel()
		}
		s.queue.close()
	}
	s.mu.Unlock()
	done := make(chan struct{})
	go func() {
		s.workers.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Service) IsInfoCached(id string) bool { return s.infoCache.Has(id) }

func (s *Service) QueryInfo(ctx context.Context, id string) (InfoResponse, error) {
	var cached InfoResponse
	if found, err := s.infoCache.Get(id, &cached); err == nil && found {
		return cached, nil
	}
	var photo PhotoInfo
	var album AlbumInfo
	var photoErr, albumErr error
	var queries sync.WaitGroup
	queries.Add(2)
	go func() {
		defer queries.Done()
		photo, photoErr = s.upstream.QueryPhoto(ctx, id)
	}()
	go func() {
		defer queries.Done()
		album, albumErr = s.upstream.QueryAlbum(ctx, id)
	}()
	queries.Wait()
	if photoErr != nil {
		return InfoResponse{}, translateError(photoErr)
	}
	cover, coverErr := s.images.DownloadCover(ctx, photo)
	if coverErr != nil {
		cover = nil
	}
	response := InfoResponse{Name: photo.Name}
	if albumErr == nil {
		response.Description = stringPtr(album.Description)
		response.Views = stringPtr(album.TotalViews)
		response.Likes = stringPtr(album.Likes)
		response.Authors = album.Authors
		response.Tags = album.Tags
		response.Works = album.Works
		response.Actors = album.Actors
	}
	if len(cover) > 0 {
		encoded := imageDataURL(cover)
		response.Cover = &encoded
	}
	if err := s.infoCache.Set(id, response); err != nil {
		fmt.Printf("Failed to cache info %s: %v\n", id, err)
	}
	return response, nil
}

func (s *Service) EnqueuePDF(id string) TaskStatusResult {
	s.mu.Lock()
	shutting := s.shutting
	s.mu.Unlock()
	if shutting {
		return TaskStatusResult{Status: "error", Error: "Service shutting down"}
	}
	status := s.pdfStatus(id)
	if status.Status != "not_found" {
		return status
	}
	if !s.queue.push(id) {
		return TaskStatusResult{Status: "error", Error: "Queue full"}
	}
	s.states.set(id, taskState{status: "pending", updatedAt: time.Now()})
	return TaskStatusResult{Status: "pending"}
}

func (s *Service) PDFStatus(id string) TaskStatusResult { return s.pdfStatus(id) }

func (s *Service) ReadPDF(id string) ([]byte, error) {
	data, found, err := s.pdfCache.Get(id)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, errors.New("PDF not cached")
	}
	return data, nil
}

func (s *Service) pdfStatus(id string) TaskStatusResult {
	if s.pdfCache.Has(id) {
		return TaskStatusResult{Status: "ready"}
	}
	state, found := s.states.get(id)
	if !found {
		return TaskStatusResult{Status: "not_found"}
	}
	result := TaskStatusResult{Status: state.status}
	if state.status == "error" {
		result.Error = state.error
	}
	if state.status == "processing" && state.totalImages > 0 {
		progress := &TaskProgress{TotalImages: state.totalImages, ProcessedImages: state.processedImages, StartedAt: state.startedAt.UnixMilli()}
		if state.etaSeconds > 0 {
			progress.ETAPSeconds = &state.etaSeconds
		}
		result.Progress = progress
	}
	return result
}

func (s *Service) runWorker(workerID int) {
	defer s.workers.Done()
	for s.queue.wait(s.workerCtx) {
		id, ok := s.queue.pop()
		if !ok {
			continue
		}
		state, _ := s.states.get(id)
		s.states.set(id, taskState{status: "processing", retryCount: state.retryCount, updatedAt: time.Now()})
		if err := s.generateTask(s.workerCtx, id, state.retryCount); err != nil {
			if errors.Is(err, context.Canceled) || s.workerCtx.Err() != nil {
				return
			}
			message := translateError(err)
			fmt.Printf("Failed to generate PDF for %s (worker %d): %v\n", id, workerID, err)
			if state.retryCount < s.cfg.MaxRetries && s.queue.push(id) {
				s.states.set(id, taskState{status: "pending", retryCount: state.retryCount + 1, updatedAt: time.Now()})
			} else if state.retryCount < s.cfg.MaxRetries {
				s.states.set(id, taskState{status: "error", error: "Queue full on retry", retryCount: state.retryCount, updatedAt: time.Now()})
			} else {
				s.states.set(id, taskState{status: "error", error: extractErrorMessage(message), retryCount: state.retryCount, updatedAt: time.Now()})
			}
			continue
		}
		s.states.delete(id)
		fmt.Printf("PDF generated and cached: %s\n", id)
	}
}

func (s *Service) generateTask(ctx context.Context, id string, retryCount int) error {
	photo, err := s.upstream.QueryPhoto(ctx, id)
	if err != nil {
		return err
	}
	started := time.Now()
	s.states.set(id, taskState{status: "processing", retryCount: retryCount, totalImages: len(photo.Images), startedAt: started, updatedAt: time.Now()})
	pdf, err := s.images.GeneratePDF(
		ctx,
		photo,
		s.cfg.ImageDownloadRetries,
		s.cfg.NetworkConcurrency,
		s.cfg.CPUConcurrency,
		func(progress ProgressInfo) {
			s.states.set(id, taskState{status: "processing", retryCount: retryCount, totalImages: len(photo.Images), startedAt: started, processedImages: progress.Processed, updatedAt: time.Now()})
		},
		func(elapsed time.Duration, total int) {
			eta := int(math.Round(float64(elapsed.Milliseconds()*int64(total)) / float64(s.cfg.CPUConcurrency) / 1000))
			s.states.set(id, taskState{status: "processing", retryCount: retryCount, totalImages: total, startedAt: started, processedImages: 1, etaSeconds: eta, updatedAt: time.Now()})
		},
	)
	if err != nil {
		return err
	}
	return s.pdfCache.Set(id, pdf)
}

func translateError(err error) error {
	if err == nil {
		return nil
	}
	if strings.Contains(err.Error(), "No images could be embedded") {
		return errors.New("本子没有可用图片，无法生成 PDF")
	}
	return errors.New("未找到该本子，请检查 ID 是否正确")
}

func extractErrorMessage(err error) string {
	if err == nil {
		return "未知错误"
	}
	return err.Error()
}

func stringPtr(value string) *string { return &value }
