package storage

import "io"

type CloudStorage interface {
	Upload(file io.Reader, filename string, bucket string, contentType string) error
	Read(filename, bucket string) (string, error)
	Delete(bucket string, filepath string) error
	Close()
}
