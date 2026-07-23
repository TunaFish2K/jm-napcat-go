package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type InfoCache struct {
	dir string
}

func NewInfoCache(dir string) *InfoCache { return &InfoCache{dir: dir} }

func (c *InfoCache) Init() error { return os.MkdirAll(c.dir, 0o755) }

func (c *InfoCache) Has(id string) bool {
	_, err := os.Stat(filepath.Join(c.dir, cacheName(id, ".json")))
	return err == nil
}

func (c *InfoCache) Get(id string, target any) (bool, error) {
	data, err := os.ReadFile(filepath.Join(c.dir, cacheName(id, ".json")))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if err := json.Unmarshal(data, target); err != nil {
		return false, err
	}
	return true, nil
}

func (c *InfoCache) Set(id string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	// The temporary file is kept beside the cache file so rename is atomic.
	tmp := filepath.Join(c.dir, cacheName(id, ".json")+".tmp")
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(c.dir, cacheName(id, ".json"))); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

type PDFRecord struct {
	ID        string `json:"id"`
	Size      int64  `json:"size"`
	CreatedAt int64  `json:"createdAt"`
}

type pdfManifest struct {
	Version int         `json:"version"`
	Records []PDFRecord `json:"records"`
}

type PDFCache struct {
	dir     string
	maxSize int64
	mu      sync.RWMutex
	records []PDFRecord
	total   int64
}

func NewPDFCache(dir string, maxSize int64) *PDFCache {
	return &PDFCache{dir: dir, maxSize: maxSize}
}

func (c *PDFCache) Init() error {
	if err := os.MkdirAll(c.dir, 0o755); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.loadLocked()
}

func (c *PDFCache) Has(id string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if !c.hasRecordLocked(id) {
		return false
	}
	_, err := os.Stat(c.filePath(id))
	return err == nil
}

func (c *PDFCache) Get(id string) ([]byte, bool, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if !c.hasRecordLocked(id) {
		return nil, false, nil
	}
	data, err := os.ReadFile(c.filePath(id))
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return data, true, nil
}

func (c *PDFCache) Set(id string, data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	path := c.filePath(id)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}

	for index, record := range c.records {
		if record.ID == id {
			c.total -= record.Size
			c.records = append(c.records[:index], c.records[index+1:]...)
			break
		}
	}
	c.records = append(c.records, PDFRecord{ID: id, Size: int64(len(data)), CreatedAt: time.Now().UnixMilli()})
	c.total += int64(len(data))
	if err := c.evictLocked(); err != nil {
		return err
	}
	return c.saveLocked()
}

func (c *PDFCache) Delete(id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for index, record := range c.records {
		if record.ID != id {
			continue
		}
		c.total -= record.Size
		c.records = append(c.records[:index], c.records[index+1:]...)
		break
	}
	if err := os.Remove(c.filePath(id)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return c.saveLocked()
}

func (c *PDFCache) Stats() (total, max int64, count int) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.total, c.maxSize, len(c.records)
}

func (c *PDFCache) hasRecordLocked(id string) bool {
	for _, record := range c.records {
		if record.ID == id {
			return true
		}
	}
	return false
}

func (c *PDFCache) filePath(id string) string {
	return filepath.Join(c.dir, cacheName(id, ".pdf"))
}

func (c *PDFCache) loadLocked() error {
	data, err := os.ReadFile(filepath.Join(c.dir, "manifest.json"))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var manifest pdfManifest
	if err := json.Unmarshal(data, &manifest); err != nil || manifest.Version != 1 {
		return nil
	}
	c.records = nil
	c.total = 0
	for _, record := range manifest.Records {
		if record.ID == "" || record.Size < 0 {
			continue
		}
		info, statErr := os.Stat(c.filePath(record.ID))
		if statErr != nil {
			continue
		}
		record.Size = info.Size()
		c.records = append(c.records, record)
		c.total += record.Size
	}
	return c.evictLockedAndSaveIfNeeded()
}

func (c *PDFCache) evictLockedAndSaveIfNeeded() error {
	changed := false
	for len(c.records) > 0 && c.total > c.maxSize {
		oldest := c.records[0]
		c.records = c.records[1:]
		c.total -= oldest.Size
		_ = os.Remove(c.filePath(oldest.ID))
		changed = true
	}
	if changed {
		return c.saveLocked()
	}
	return nil
}

func (c *PDFCache) evictLocked() error {
	for len(c.records) > 0 && c.total > c.maxSize {
		oldest := c.records[0]
		c.records = c.records[1:]
		c.total -= oldest.Size
		if err := os.Remove(c.filePath(oldest.ID)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

func (c *PDFCache) saveLocked() error {
	data, err := json.Marshal(pdfManifest{Version: 1, Records: c.records})
	if err != nil {
		return err
	}
	path := filepath.Join(c.dir, "manifest.json")
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

func cacheName(id, suffix string) string {
	sum := sha256.Sum256([]byte(id))
	return hex.EncodeToString(sum[:]) + suffix
}

func cacheStatsString(cache *PDFCache) string {
	total, max, count := cache.Stats()
	return fmt.Sprintf("%d records, %.1fMB / %.1fGB", count, float64(total)/1024/1024, float64(max)/1024/1024/1024)
}
