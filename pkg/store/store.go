package store

import (
	"bytes"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"github.com/k8-proxy/k8-go-comm/pkg/minio"
	min "github.com/minio/minio-go/v7"
)

const (
	MinioEndpoint     = "localhost:9000"
	MinioAccessKey    = "minioadmin"
	MinioSecretKey    = "minioadmin"
	SourceMinioBucket = "test"
)

var (
	cl *min.Client
)

func Init() {
	var err error
	cl, err = minio.NewMinioClient(MinioEndpoint, MinioAccessKey, MinioSecretKey, false)
	if err != nil {
		log.Println("minio server not found \n", err)
	}

}

func St(file []byte, filename string) (string, error) {
	exist, err := minio.CheckIfBucketExists(cl, SourceMinioBucket)
	if err != nil || !exist {
		log.Println("error checkbucket ", err)
		err = minio.CreateNewBucket(cl, "test")
		if err != nil {
			log.Println(err)
			return "", err
		}

	}
	_, errm := minio.UploadFileToMinio(cl, SourceMinioBucket, filename, bytes.NewReader(file))
	if errm != nil {
		log.Println(errm)
		return "", errm
	}
	expirein := time.Second * 60 * 2
	urlx, err := minio.GetPresignedURLForObject(cl, SourceMinioBucket, filename, expirein)
	if err != nil {
		log.Println(err)
		return "", err

	}
	return urlx.String(), nil

}

func Getfile(url string) ([]byte, error) {

	f := []byte{}
	resp, err := http.Get(url)
	if err != nil {
		return f, err
	}
	defer resp.Body.Close()

	f, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return f, err
	}
	return f, nil

}
