package gos3lambdaelasticsearch

import (
	"context"
	"io"
	"log"
	"os"

	"github.com/aws/aws-lambda-go/events"
)

func handler(ctx context.Context, s3Event events.S3Event) {
	if os.Getenv("LINE_FORMAT") == "" {
		panic("Required Environment Missing: LINE_FORMAT")
	}

	elasticSearchClient := getElasticSearchClient()
	s3Client := getS3Client()

	for _, record := range s3Event.Records {
		s3 := record.S3
		log.Printf("[%s - %s] Bucket = %s, Key = %s \n", record.EventSource, record.EventTime, s3.Bucket.Name, s3.Object.Key)

		reader, err := downloadS3FileAndGunzip(s3Client, s3.Bucket.Name, s3.Object.Key)

		if err != nil {
			continue
		}

		for {
			rec, err := reader.Read()
			if err == io.EOF {
				break
			}

			insertLine(elasticSearchClient, rec)
		}
	}
}
