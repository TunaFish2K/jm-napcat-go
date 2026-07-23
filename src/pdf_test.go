package main

import (
	"bytes"
	"context"
	"image"
	"image/color"
	"image/jpeg"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/pdfcpu/pdfcpu/pkg/api"
	"github.com/pdfcpu/pdfcpu/pkg/pdfcpu/model"
)

func TestBuildPDFKeepsPageOrderAndDimensions(t *testing.T) {
	first := jpegImage(t, 20, 30, color.RGBA{R: 255, A: 255})
	second := jpegImage(t, 40, 50, color.RGBA{B: 255, A: 255})
	pdf, err := buildPDF([]pdfPage{{Image: first, Width: 20, Height: 30}, {Image: second, Width: 40, Height: 50}})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(pdf, []byte("%PDF-1.4")) {
		t.Fatal("missing PDF header")
	}
	if !bytes.Contains(pdf, []byte("/Count 2")) || !bytes.Contains(pdf, []byte("/MediaBox [0 0 20 30]")) || !bytes.Contains(pdf, []byte("/MediaBox [0 0 40 50]")) {
		t.Fatal("PDF does not contain expected page metadata")
	}
	if err := api.Validate(bytes.NewReader(pdf), model.NewDefaultConfiguration()); err != nil {
		t.Fatalf("generated PDF failed validation: %v", err)
	}
}

func TestEncryptPDFCanBeDecryptedWithID(t *testing.T) {
	pdf, err := buildPDF([]pdfPage{{Image: jpegImage(t, 10, 10, color.RGBA{G: 255, A: 255}), Width: 10, Height: 10}})
	if err != nil {
		t.Fatal(err)
	}
	encrypted, err := EncryptPDF(pdf, "12345")
	if err != nil {
		t.Fatal(err)
	}
	var decrypted bytes.Buffer
	configuration := model.NewAESConfiguration("12345", "12345", 256)
	if err := api.Decrypt(bytes.NewReader(encrypted), &decrypted, configuration); err != nil {
		t.Fatalf("decrypt failed: %v", err)
	}
	if err := api.Validate(bytes.NewReader(decrypted.Bytes()), model.NewDefaultConfiguration()); err != nil {
		t.Fatalf("decrypted PDF failed validation: %v", err)
	}
}

func TestGeneratePDFDownloadsAndEmbedsImagesInInputOrder(t *testing.T) {
	first := jpegImage(t, 8, 9, color.RGBA{R: 255, A: 255})
	second := jpegImage(t, 10, 11, color.RGBA{B: 255, A: 255})
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "image/jpeg")
		if request.URL.Path == "/first" {
			_, _ = writer.Write(first)
			return
		}
		_, _ = writer.Write(second)
	}))
	defer server.Close()
	cfg := DefaultConfig()
	processor := NewImageProcessor(cfg)
	processor.client = server.Client()
	var progress []ProgressInfo
	pdf, err := processor.GeneratePDF(context.Background(), PhotoInfo{ID: "999999", Images: []ImageInfo{
		{Name: "first.jpg", URL: server.URL + "/first"},
		{Name: "second.jpg", URL: server.URL + "/second"},
	}}, 1, 2, 2, func(value ProgressInfo) { progress = append(progress, value) }, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(progress) != 2 || progress[0].Processed != 1 || progress[1].Processed != 2 {
		t.Fatalf("progress = %#v", progress)
	}
	firstPage := bytes.Index(pdf, []byte("/MediaBox [0 0 8 9]"))
	secondPage := bytes.Index(pdf, []byte("/MediaBox [0 0 10 11]"))
	if firstPage < 0 || secondPage < 0 || firstPage > secondPage {
		t.Fatal("PDF pages are not in input order")
	}
}

func jpegImage(t *testing.T, width, height int, fill color.RGBA) []byte {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, width, height))
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			img.SetRGBA(x, y, fill)
		}
	}
	var buffer bytes.Buffer
	if err := jpeg.Encode(&buffer, img, &jpeg.Options{Quality: 90}); err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}
