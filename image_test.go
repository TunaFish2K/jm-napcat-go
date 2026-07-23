package main

import (
	"image"
	"image/color"
	"testing"
)

func TestGetSliceCountBranches(t *testing.T) {
	if got := GetSliceCount(10, 9, "page.jpg"); got != 0 {
		t.Fatalf("scramble branch = %d", got)
	}
	if got := GetSliceCount(1, 268849, "page.jpg"); got != 10 {
		t.Fatalf("old photo branch = %d", got)
	}
	if got := GetSliceCount(1, 268850, "page.gif"); got != 0 {
		t.Fatalf("gif branch = %d", got)
	}
	if got := GetSliceCount(1, 268850, "page.GIF"); got < 2 || got > 20 || got%2 != 0 {
		t.Fatalf("new photo branch = %d", got)
	}
}

func TestReverseImageBySlicePreservesSliceOrder(t *testing.T) {
	input := image.NewRGBA(image.Rect(0, 0, 1, 6))
	colors := []color.RGBA{
		{R: 255, A: 255},
		{G: 255, A: 255},
		{B: 255, A: 255},
		{R: 255, G: 255, A: 255},
		{R: 255, B: 255, A: 255},
		{G: 255, B: 255, A: 255},
	}
	for y, value := range colors {
		input.SetRGBA(0, y, value)
	}
	output, err := ReverseImageBySlice(input, 3)
	if err != nil {
		t.Fatal(err)
	}
	for y, sourceY := range []int{4, 5, 2, 3, 0, 1} {
		got := color.RGBAModel.Convert(output.At(0, y))
		want := color.RGBAModel.Convert(colors[sourceY])
		if got != want {
			t.Fatalf("row %d = %#v, want %#v", y, got, want)
		}
	}
}

func TestReverseImageBySliceHandlesRemainder(t *testing.T) {
	input := image.NewRGBA(image.Rect(0, 0, 1, 7))
	for y := 0; y < 7; y++ {
		input.SetRGBA(0, y, color.RGBA{R: uint8(y), A: 255})
	}
	output, err := ReverseImageBySlice(input, 3)
	if err != nil {
		t.Fatal(err)
	}
	for y, sourceY := range []int{4, 5, 6, 2, 3, 0, 1} {
		got := color.RGBAModel.Convert(output.At(0, y))
		want := color.RGBAModel.Convert(input.At(0, sourceY))
		if got != want {
			t.Fatalf("row %d = %#v, want %#v", y, got, want)
		}
	}
}
