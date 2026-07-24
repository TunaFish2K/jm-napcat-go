package main

import (
	"bytes"
	"context"
	"fmt"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"sync"
	"time"

	_ "golang.org/x/image/webp"
)

type ProgressInfo struct {
	Processed int
	Total     int
}

type processedImage struct {
	Data   []byte
	Width  int
	Height int
	Name   string
}

func (p *ImageProcessor) GeneratePDF(
	ctx context.Context,
	photo PhotoInfo,
	imageRetries int,
	networkConcurrency int,
	cpuConcurrency int,
	onProgress func(ProgressInfo),
	onFirstImage func(elapsed time.Duration, total int),
) ([]byte, error) {
	if len(photo.Images) == 0 {
		return nil, fmt.Errorf("No images could be embedded into PDF")
	}
	started := time.Now()
	appLog.Info("PDF generation started", "photo_id", photo.ID, "images", len(photo.Images), "network_concurrency", networkConcurrency, "cpu_concurrency", cpuConcurrency, "retries", imageRetries)
	network := make(chan struct{}, max(1, networkConcurrency))
	cpu := make(chan struct{}, max(1, cpuConcurrency))
	results := make([]*processedImage, len(photo.Images))
	completed := make(chan int, len(photo.Images))
	var wg sync.WaitGroup
	var firstMu sync.Mutex
	firstReported := false

	for index, item := range photo.Images {
		index, item := index, item
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { completed <- index }()
			raw, err := p.downloadWithRetry(ctx, item.URL, imageRetries, network)
			if err != nil {
				appLog.Warn("PDF image download failed", "photo_id", photo.ID, "image", item.Name, "index", index+1, "error", err)
				return
			}
			select {
			case cpu <- struct{}{}:
			case <-ctx.Done():
				return
			}
			processed, processErr := p.Process(raw, photo.ScrambleID, parsePhotoID(photo.ID), item.Name)
			<-cpu
			if processErr != nil {
				appLog.Warn("PDF image processing failed", "photo_id", photo.ID, "image", item.Name, "index", index+1, "error", processErr)
				return
			}
			config, _, configErr := image.DecodeConfig(bytes.NewReader(processed))
			if configErr != nil {
				appLog.Warn("processed PDF image metadata failed", "photo_id", photo.ID, "image", item.Name, "index", index+1, "error", configErr)
				return
			}
			results[index] = &processedImage{Data: processed, Width: config.Width, Height: config.Height, Name: item.Name}
			appLog.Debug("PDF image processed", "photo_id", photo.ID, "image", item.Name, "index", index+1, "bytes", len(processed))
			firstMu.Lock()
			if !firstReported {
				firstReported = true
				if onFirstImage != nil {
					onFirstImage(time.Since(started), len(photo.Images))
				}
			}
			firstMu.Unlock()
		}()
	}

	pages := make([]pdfPage, 0, len(results))
	ready := make([]bool, len(results))
	nextToEmbed := 0
	for completedCount := 0; completedCount < len(results); completedCount++ {
		index := <-completed
		ready[index] = true
		for nextToEmbed < len(results) && ready[nextToEmbed] {
			item := results[nextToEmbed]
			if item != nil {
				pages = append(pages, pdfPage{Image: item.Data, Width: item.Width, Height: item.Height})
				if onProgress != nil {
					onProgress(ProgressInfo{Processed: len(pages), Total: len(photo.Images)})
				}
			}
			nextToEmbed++
		}
	}
	wg.Wait()
	if len(pages) == 0 {
		appLog.Error("PDF generation produced no pages", "photo_id", photo.ID, "elapsed", time.Since(started))
		return nil, fmt.Errorf("No images could be embedded into PDF")
	}
	pdf, err := buildPDF(pages)
	if err != nil {
		appLog.Error("PDF assembly failed", "photo_id", photo.ID, "pages", len(pages), "elapsed", time.Since(started), "error", err)
		return nil, err
	}
	appLog.Info("PDF generation completed", "photo_id", photo.ID, "pages", len(pages), "bytes", len(pdf), "elapsed", time.Since(started))
	return pdf, nil
}

func (p *ImageProcessor) downloadWithRetry(ctx context.Context, rawURL string, retries int, pool chan struct{}) ([]byte, error) {
	if retries < 1 {
		retries = 1
	}
	var lastErr error
	for attempt := 1; attempt <= retries; attempt++ {
		select {
		case pool <- struct{}{}:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		data, err := p.Download(ctx, rawURL)
		<-pool
		if err == nil {
			return data, nil
		}
		lastErr = err
		if attempt < retries {
			appLog.Warn("PDF image download retry", "url", shortText(rawURL, 180), "attempt", attempt+1, "max_attempts", retries, "error", err)
			timer := time.NewTimer(time.Duration(attempt) * time.Second)
			select {
			case <-timer.C:
			case <-ctx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				return nil, ctx.Err()
			}
		}
	}
	return nil, lastErr
}

type pdfPage struct {
	Image         []byte
	Width, Height int
}

func buildPDF(pages []pdfPage) ([]byte, error) {
	if len(pages) == 0 {
		return nil, fmt.Errorf("No images could be embedded into PDF")
	}
	objectCount := 2 + len(pages)*3
	objects := make([][]byte, objectCount+1)
	pageReferences := make([]string, 0, len(pages))
	for index, page := range pages {
		pageObject := 3 + index*3
		contentObject := pageObject + 1
		imageObject := pageObject + 2
		pageReferences = append(pageReferences, fmt.Sprintf("%d 0 R", pageObject))
		content := fmt.Sprintf("q\n%d 0 0 %d 0 0 cm\n/Im0 Do\nQ\n", page.Width, page.Height)
		objects[pageObject] = []byte(fmt.Sprintf("<< /Type /Page /Parent 2 0 R /MediaBox [0 0 %d %d] /Resources << /XObject << /Im0 %d 0 R >> >> /Contents %d 0 R >>", page.Width, page.Height, imageObject, contentObject))
		objects[contentObject] = []byte(fmt.Sprintf("<< /Length %d >>\nstream\n%sendstream", len(content), content))
		objects[imageObject] = []byte(fmt.Sprintf("<< /Type /XObject /Subtype /Image /Width %d /Height %d /ColorSpace /DeviceRGB /BitsPerComponent 8 /Filter /DCTDecode /Length %d >>\nstream\n%s\nendstream", page.Width, page.Height, len(page.Image), page.Image))
	}
	objects[1] = []byte("<< /Type /Catalog /Pages 2 0 R >>")
	objects[2] = []byte(fmt.Sprintf("<< /Type /Pages /Kids [%s] /Count %d >>", joinStrings(pageReferences), len(pages)))

	var output bytes.Buffer
	output.WriteString("%PDF-1.4\n%\xE2\xE3\xCF\xD3\n")
	offsets := make([]int, objectCount+1)
	for index := 1; index <= objectCount; index++ {
		offsets[index] = output.Len()
		fmt.Fprintf(&output, "%d 0 obj\n", index)
		output.Write(objects[index])
		output.WriteString("\nendobj\n")
	}
	xrefOffset := output.Len()
	fmt.Fprintf(&output, "xref\n0 %d\n", objectCount+1)
	output.WriteString("0000000000 65535 f \n")
	for index := 1; index <= objectCount; index++ {
		fmt.Fprintf(&output, "%010d 00000 n \n", offsets[index])
	}
	fmt.Fprintf(&output, "trailer\n<< /Size %d /Root 1 0 R >>\nstartxref\n%d\n%%%%EOF\n", objectCount+1, xrefOffset)
	return output.Bytes(), nil
}

func joinStrings(items []string) string {
	result := ""
	for index, item := range items {
		if index > 0 {
			result += " "
		}
		result += item
	}
	return result
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
