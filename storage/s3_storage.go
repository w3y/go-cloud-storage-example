package storage

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"io"
	"os"
	"time"
)

const DefaultLargeDownloaderOptionSizeInMB int64 = 256

type S3Storage struct {
	Sess *session.Session
	S3   *s3.S3
}

type S3CloudStorageInterface interface {
	Upload(file io.Reader, filename string, bucket string, contentType string) error
	Read(filename, bucket string) (string, error)
	GetListBucketItems(bucket string, prefix string) ([]*s3.Object, error)
	Delete(bucket string, filepath string) error
}

func NewS3CloudStorage(awsAccessKeyID, awsSecretAccessKey, awsSessionToken, awsRegion string) (*S3Storage, error) {

	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(awsRegion),
		Credentials: credentials.NewStaticCredentials(awsAccessKeyID, awsSecretAccessKey, awsSessionToken),
	})

	if err != nil {
		return nil, err
	}

	svc := s3.New(sess)

	storage := &S3Storage{
		Sess: sess,
		S3:   svc,
	}

	return storage, nil
}

func (s *S3Storage) Read(filename, bucket string) (string, error) {
	buff := &aws.WriteAtBuffer{}
	downloader := s3manager.NewDownloader(s.Sess)
	_, err := downloader.Download(buff, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(filename),
	})

	if err != nil {
		return "", err
	}

	return string(buff.Bytes()), nil
}

func (s *S3Storage) ListBuckets() {
	result, err := s.S3.ListBuckets(nil)

	if err != nil {
		fmt.Println(err)
	}

	fmt.Println("Buckets:")

	for _, b := range result.Buckets {
		fmt.Printf("* %s created on %s\n",
			aws.StringValue(b.Name), aws.TimeValue(b.CreationDate))
	}
}

func (s *S3Storage) ListBucketItems(bucket string, prefix string) {
	resp, err := s.S3.ListObjects(&s3.ListObjectsInput{Bucket: aws.String(bucket), Prefix: aws.String(prefix)})

	if err != nil {
		return
	}

	for _, item := range resp.Contents {
		fmt.Println("Name:         ", *item.Key)
		fmt.Println("Last modified:", *item.LastModified)
		fmt.Println("Size:         ", *item.Size)
		fmt.Println("Storage class:", *item.StorageClass)
		fmt.Println("")
	}
}

func (s *S3Storage) GetListBucketItems(bucket string, prefix string) ([]*s3.Object, error) {
	var results []*s3.Object

	resp, err := s.S3.ListObjects(&s3.ListObjectsInput{Bucket: aws.String(bucket), Prefix: aws.String(prefix)})

	if err != nil {
		return results, err
	}

	return resp.Contents, nil
}

func (s *S3Storage) Upload(file io.Reader, filename string, bucket string, contentType string) error {
	uploader := s3manager.NewUploader(s.Sess)
	lastModified := time.Now().String()
	_, err := uploader.Upload(&s3manager.UploadInput{
		ACL:         aws.String(s3.ObjectCannedACLPublicRead),
		Bucket:      aws.String(bucket),
		Key:         aws.String(filename),
		Body:        file,
		ContentType: aws.String(contentType),
		Metadata: map[string]*string{
			"Last-Modified": &lastModified,
		},
	})

	if err != nil {
		return err
	}

	return nil
}

func (s *S3Storage) Delete(bucket string, filepath string) error {
	_, err := s.S3.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(filepath),
	})

	if err != nil {
		return err
	}

	err = s.S3.WaitUntilObjectNotExists(&s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(filepath),
	})

	if err != nil {
		return err
	}

	return nil
}

func (s *S3Storage) UploadFileWithOption(options s3manager.UploadInput) error {
	uploader := s3manager.NewUploader(s.Sess)
	_, err := uploader.Upload(&options)
	if err != nil {
		return err
	}

	return nil
}

func (s *S3Storage) DownloadFile(bucket string, filepath string) (*aws.WriteAtBuffer, error) {
	data := &aws.WriteAtBuffer{}
	downloader := s3manager.NewDownloader(s.Sess)
	_, err := downloader.Download(
		data,
		&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(filepath),
		},
	)
	return data, err
}

func (s *S3Storage) DownloadAndWriteStreamToFile(bucket string, filePath string, f *os.File) error {

	downloader := s3manager.NewDownloader(s.Sess, func(d *s3manager.Downloader) { d.PartSize = DefaultLargeDownloaderOptionSizeInMB * 1024 * 1024 })

	_, err := downloader.Download(
		f,
		&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(filePath),
		},
	)
	return err
}

func (s *S3Storage) MoveFileInsideBucket(bucket string, fromFilePath string, toFilePath string) error {
	fromFilePathBucket := fmt.Sprintf("%v/%v", bucket, fromFilePath)
	input := &s3.CopyObjectInput{
		Bucket:     aws.String(bucket),
		CopySource: aws.String(fromFilePathBucket),
		Key:        aws.String(toFilePath),
	}

	_, err := s.S3.CopyObject(input)
	if err != nil {
		return err
	}
	err = s.Delete(bucket, fromFilePath)
	if err != nil {
		return err
	}
	return nil
}
