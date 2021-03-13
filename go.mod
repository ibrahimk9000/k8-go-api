module github.com/k8-proxy/k8-go-api

go 1.16

require (
	github.com/gorilla/mux v1.8.0
	github.com/justinas/alice v1.2.0
	github.com/k8-proxy/k8-go-comm v0.1.3
	github.com/minio/minio-go/v7 v7.0.10
	github.com/rs/zerolog v1.20.0
	github.com/streadway/amqp v1.0.0

)

replace github.com/k8-proxy/k8-go-comm v0.1.3 => /home/ibrahim/comm/k8-go-comm
