package main

import (
	"bytes"

	"github.com/pdfcpu/pdfcpu/pkg/api"
	"github.com/pdfcpu/pdfcpu/pkg/pdfcpu/model"
)

func EncryptPDF(data []byte, password string) ([]byte, error) {
	configuration := model.NewAESConfiguration(password, password, 256)
	configuration.Permissions = model.PermissionsAll
	var encrypted bytes.Buffer
	if err := api.Encrypt(bytes.NewReader(data), &encrypted, configuration); err != nil {
		return nil, err
	}
	return encrypted.Bytes(), nil
}
