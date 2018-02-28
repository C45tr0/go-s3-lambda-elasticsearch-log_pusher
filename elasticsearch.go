package gos3lambdaelasticsearch

import (
	"log"
	"os"

	"github.com/olivere/elastic"
	"github.com/satyrius/gonx"
)

func getElasticSearchClient() *elastic.Client {
	if os.Getenv("ELASTICSEARCH_URL") == "" {
		panic("Required Environment Missing: ELASTICSEARCH_URL")
	}

	elasticSearchURL := os.Getenv("ELASTICSEARCH_URL")

	client, err := elastic.NewClient(elastic.SetURL(elasticSearchURL))
	if err != nil {
		panic(err)
	}

	return client
}

func insertLine(client *elastic.Client, line *gonx.Entry) {
	log.Println(line)
}
