package main

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"image"
	"image/color"
	"image/draw"
	"image/gif"
	"image/jpeg"
	"image/png"
	"io"
	"net/http"
	"strings"
	"time"

	xdraw "golang.org/x/image/draw"
	_ "golang.org/x/image/webp"
)

type ImageProcessor struct {
	client  *http.Client
	timeout time.Duration
}

func NewImageProcessor(cfg Config) *ImageProcessor {
	return &ImageProcessor{client: &http.Client{}, timeout: time.Duration(cfg.ImageDownloadTimeoutMS) * time.Millisecond}
}

func GetSliceCount(scrambleID, photoID int, filename string) int {
	if photoID < scrambleID || strings.HasSuffix(filename, ".gif") {
		return 0
	}
	if photoID < 268850 {
		return 10
	}
	hash := md5.Sum([]byte(fmt.Sprintf("%d%s", photoID, strings.Split(filename, ".")[0])))
	lastHexCharacter := hex.EncodeToString(hash[:])[31]
	modulus := 8
	if photoID < 421926 {
		modulus = 10
	}
	return (int(lastHexCharacter)%modulus)*2 + 2
}

func decodeImage(data []byte) (image.Image, error) {
	decoded, _, err := image.Decode(bytes.NewReader(data))
	return decoded, err
}

func flattenWhite(src image.Image) *image.RGBA {
	bounds := src.Bounds()
	dst := image.NewRGBA(image.Rect(0, 0, bounds.Dx(), bounds.Dy()))
	draw.Draw(dst, dst.Bounds(), image.NewUniform(color.White), image.Point{}, draw.Src)
	draw.Draw(dst, dst.Bounds(), src, bounds.Min, draw.Over)
	return dst
}

func encodeJPEG(src image.Image, quality int) ([]byte, error) {
	var buffer bytes.Buffer
	if err := jpeg.Encode(&buffer, flattenWhite(src), &jpeg.Options{Quality: quality}); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func ReverseImageBySlice(src image.Image, sliceCount int) (image.Image, error) {
	if sliceCount <= 0 {
		return src, nil
	}
	bounds := src.Bounds()
	width, height := bounds.Dx(), bounds.Dy()
	if width <= 0 || height <= 0 {
		return nil, fmt.Errorf("invalid image dimensions %dx%d", width, height)
	}
	over := height % sliceCount
	move := height / sliceCount
	dst := image.NewRGBA(image.Rect(0, 0, width, height))
	draw.Draw(dst, dst.Bounds(), image.NewUniform(color.White), image.Point{}, draw.Src)
	for i := 0; i < sliceCount; i++ {
		sourceY := height - move*(i+1) - over
		sliceHeight := move
		if i == 0 {
			sliceHeight += over
		}
		destinationY := move * i
		if i != 0 {
			destinationY += over
		}
		sourcePoint := image.Point{X: bounds.Min.X, Y: bounds.Min.Y + sourceY}
		destination := image.Rect(0, destinationY, width, destinationY+sliceHeight)
		draw.Draw(dst, destination, src, sourcePoint, draw.Over)
	}
	return dst, nil
}

func (p *ImageProcessor) Download(ctx context.Context, rawURL string) ([]byte, error) {
	requestCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	request, err := http.NewRequestWithContext(requestCtx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, err
	}
	response, err := p.client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, fmt.Errorf("failed to download image: %s", response.Status)
	}
	return io.ReadAll(response.Body)
}

func (p *ImageProcessor) Process(raw []byte, scrambleID, photoID int, filename string) ([]byte, error) {
	decoded, err := decodeImage(raw)
	if err != nil {
		return nil, err
	}
	if slices := GetSliceCount(scrambleID, photoID, filename); slices > 0 {
		decoded, err = ReverseImageBySlice(decoded, slices)
		if err != nil {
			return nil, err
		}
	}
	return encodeJPEG(decoded, 90)
}

func (p *ImageProcessor) ProcessSafe(raw []byte, scrambleID, photoID int, filename string) []byte {
	processed, err := p.Process(raw, scrambleID, photoID, filename)
	if err != nil {
		appLog.Warn("image processing failed", "image", filename, "error", err)
		return nil
	}
	return processed
}

func (p *ImageProcessor) DownloadCover(ctx context.Context, photo PhotoInfo) ([]byte, error) {
	if len(photo.Images) == 0 {
		return nil, nil
	}
	first := photo.Images[0]
	raw, err := p.Download(ctx, first.URL)
	if err != nil {
		return nil, err
	}
	processed, err := p.Process(raw, photo.ScrambleID, parsePhotoID(photo.ID), first.Name)
	if err != nil {
		return nil, err
	}
	decoded, _, err := image.Decode(bytes.NewReader(processed))
	if err != nil {
		return nil, err
	}
	bounds := decoded.Bounds()
	width, height := bounds.Dx(), bounds.Dy()
	if width > 1200 || height > 1200 {
		scale := 1200.0 / float64(width)
		if height > width {
			scale = 1200.0 / float64(height)
		}
		width = max(1, int(float64(width)*scale))
		height = max(1, int(float64(height)*scale))
		resized := image.NewRGBA(image.Rect(0, 0, width, height))
		xdraw.CatmullRom.Scale(resized, resized.Bounds(), decoded, bounds, draw.Over, nil)
		decoded = resized
	}
	return encodeJPEG(decoded, 80)
}

func imageDataURL(data []byte) string {
	return "data:image/jpeg;base64," + base64.StdEncoding.EncodeToString(data)
}

// Keep the standard decoders linked even when an upstream response is only
// exercised through image.Decode in another package file.
var _ = png.Decode
var _ = gif.Decode
