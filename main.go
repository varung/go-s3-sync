package main

import (
	"crypto/md5"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type ProgressReader struct {
	r   io.Reader
	sum int
}

func (s ProgressReader) Read(p []byte) (n int, err error) {
	n, err = s.r.Read(p)
	s.sum += n
	return
}

func ComputeMd5(r io.Reader) (string, error) {
	m := md5.New()
	io.Copy(m, r)
	return fmt.Sprintf("%x", m.Sum(nil)), nil
}

func IsFileUploaded(pathname string, bucket string, sess *session.Session) (bool, error) {
	// check if file is already there with correct size and md5
	svc := s3.New(sess)
	params := &s3.HeadObjectInput{
		Bucket: &bucket,
		Key:    &pathname,
	}
	resp, err := svc.HeadObject(params)
	if err != nil {
		aerr := err.(awserr.Error)
		if aerr.Code() == "NotFound" {
			return false, nil
		} else {
			return false, err
		}
	} else {
		if resp.ContentLength != nil {
			f, err := os.Open(pathname)
			if err != nil {
				return false, err
			}
			stat, err := f.Stat()
			if err != nil {
				return false, err
			}
			if *resp.ContentLength == stat.Size() {
				etag := strings.Replace(*resp.ETag, "\"", "", -1)
				sum, err := ComputeMd5(f)
				if err != nil {
					return false, err
				}
				if etag == sum {
					log.Println("already-uploaded", pathname)
					return true, nil
				} else {
					log.Println("md5-mismatch", pathname)
					return false, nil
				}
			} else {
				log.Println("size-mismatch", pathname)
				return false, nil
			}
		}
	}
	return false, nil
}

func main() {
	log.SetFlags(log.Lshortfile | log.Ldate)
	bucket := flag.String("bucket", "cloudlabs.blobs.us-west-2", "bucket to put files")
	flag.Parse()
	key := flag.Arg(0)

	log.Println(*bucket, key)

	var err error
	file, err := os.Open(key)
	if err != nil {
		log.Fatal(err)
	}
	os.Setenv("AWS_REGION", "us-west-2")
	// The session the S3 Uploader will use
	sess, err := session.NewSession()

	if uploaded, _ := IsFileUploaded(key, *bucket, sess); !uploaded {
		// Create an uploader with the session and custom options
		uploader := s3manager.NewUploader(sess, func(u *s3manager.Uploader) {
			u.PartSize = 64 * 1024 * 1024 // 64MB per part
		})

		// Upload input parameters
		upParams := &s3manager.UploadInput{
			Bucket: bucket,
			Key:    &key,
			Body:   ProgressReader{file, 0},
		}

		// Perform an upload.
		result, err := uploader.Upload(upParams)
		log.Println(result, err)
	}
}
