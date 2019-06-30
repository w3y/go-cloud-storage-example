package storage

import (
	"cloud.google.com/go/storage"
	"context"
	"errors"
	"google.golang.org/api/option"
	"io"
	"io/ioutil"
)

type GCloudStorage struct {
	ctx       context.Context
	gcClient  *storage.Client
	projectID string
	bucket    string
	name      string
	public    bool
}

func NewGCloudStorage(ctx context.Context, projectID, authFile string, public bool) (*GCloudStorage, error) {
	gcClient, err := storage.NewClient(ctx, option.WithCredentialsFile(authFile))
	if err != nil {
		return nil, err
	}
	if err != nil {
		return nil, err
	}
	return &GCloudStorage{
		ctx:       ctx,
		gcClient:  gcClient,
		projectID: projectID,
		public:    public,
	}, nil
}

func (g *GCloudStorage) Upload(r io.Reader, filename string, bucket string, contentType string) error {
	var err error
	if g.gcClient == nil {
		return errors.New("google storage client is nil")
	}
	bh := g.gcClient.Bucket(bucket)
	// Next check if the bucket exists
	if _, err = bh.Attrs(g.ctx); err != nil {
		return err
	}
	obj := bh.Object(filename)
	w := obj.NewWriter(g.ctx)
	if _, err := io.Copy(w, r); err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}

	if g.public {
		if err := obj.ACL().Set(g.ctx, storage.AllUsers, storage.RoleReader); err != nil {
			return err
		}
	}
	_, err = obj.Attrs(g.ctx)
	if err != nil {
		return err
	}
	return nil
}
func (g *GCloudStorage) Read(filename, bucket string) (string, error) {
	rc, err := g.gcClient.Bucket(bucket).Object(filename).NewReader(g.ctx)
	if err != nil {
		return "", err
	}
	defer rc.Close()
	data, err := ioutil.ReadAll(rc)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func (g *GCloudStorage) Delete(bucket string, filepath string) error {
	o := g.gcClient.Bucket(bucket).Object(filepath)
	if err := o.Delete(g.ctx); err != nil {
		return err
	}
	return nil
}

func (g *GCloudStorage) Close() {
	if g.gcClient != nil {
		g.gcClient.Close()
	}
}
