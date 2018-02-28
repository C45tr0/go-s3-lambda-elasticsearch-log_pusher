package gos3lambdaelasticsearch

import (
	"compress/gzip"
	"log"
	"os"

	"github.com/satyrius/gonx"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

func getS3Client() *s3.S3 {
	return s3.New(nil)
}

func downloadS3FileAndGunzip(client *s3.S3, bucket string, key string) (*gonx.Reader, error) {
	result, err := client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})

	if err != nil {
		log.Printf("Failed to get object: %s", err)
		return nil, err
	}

	gunzip, err := gzip.NewReader(result.Body)

	if err != nil {
		log.Printf("Failed to create gunzip: %s", err)
		return nil, err
	}

	defer result.Body.Close()
	defer gunzip.Close()

	return gonx.NewReader(gunzip, os.Getenv("LINE_FORMAT")), nil
}
