package main

import (
	"testing"
)

func TestInfoCacheRoundTrip(t *testing.T) {
	cache := NewInfoCache(t.TempDir())
	if err := cache.Init(); err != nil {
		t.Fatal(err)
	}
	want := InfoResponse{Name: "test"}
	if err := cache.Set("123", want); err != nil {
		t.Fatal(err)
	}
	if !cache.Has("123") {
		t.Fatal("cache does not contain written value")
	}
	var got InfoResponse
	found, err := cache.Get("123", &got)
	if err != nil || !found {
		t.Fatalf("Get() = found=%v, err=%v", found, err)
	}
	if got.Name != want.Name {
		t.Fatalf("got %#v, want %#v", got, want)
	}
}

func TestPDFCacheEvictsOldestRecord(t *testing.T) {
	cache := NewPDFCache(t.TempDir(), 5)
	if err := cache.Init(); err != nil {
		t.Fatal(err)
	}
	if err := cache.Set("first", []byte("1234")); err != nil {
		t.Fatal(err)
	}
	if err := cache.Set("second", []byte("5678")); err != nil {
		t.Fatal(err)
	}
	if cache.Has("first") {
		t.Fatal("oldest record was not evicted")
	}
	if !cache.Has("second") {
		t.Fatal("newest record was evicted")
	}
	if total, _, count := cache.Stats(); total != 4 || count != 1 {
		t.Fatalf("stats = total=%d count=%d", total, count)
	}
}

func TestTaskQueueDeduplicatesAndBlocksAtCapacity(t *testing.T) {
	queue := newTaskQueue(1)
	if !queue.push("first") || !queue.push("first") {
		t.Fatal("duplicate push should be accepted")
	}
	if queue.push("second") {
		t.Fatal("queue accepted an item over capacity")
	}
	id, ok := queue.pop()
	if !ok || id != "first" {
		t.Fatalf("pop() = %q, %v", id, ok)
	}
}
