package message

import (
	"log"

	"github.com/streadway/amqp"

	"github.com/k8-proxy/k8-go-comm/pkg/rabbitmq"
)

const (
	Exchange   = "process-exchange"
	RoutingKey = "process-request"
	QueueName  = "process-queue"

	Aexchange   = "adaptation-exchange"
	AroutingKey = "adaptation-request"
	AqueueName  = "adaptation-queue"

	MqHost = "localhost"
	MqPort = "5672"
)

var (
	conn *amqp.Connection
)

func Conn() *amqp.Connection {
	return conn
}

func Init() {

	var err error
	conn, err = rabbitmq.NewInstance(MqHost, MqPort, "", "")
	if err != nil {
		log.Println("rabbitmq server not found\n", err)
	}
}

func AmqpM(requestid string, url string) (string, error) {

	publisher, err := rabbitmq.NewQueuePublisher(conn, Exchange, amqp.ExchangeDirect)
	if err != nil {
		log.Println(err)
		return "", err

	}

	defer publisher.Close()

	ctable := amqp.Table{
		"x-match":    "any",
		"request-id": requestid,
	}

	// Start a consumer
	msgs, ch, err := rabbitmq.NewQueueConsumer(conn, AqueueName, Aexchange, amqp.ExchangeHeaders, AroutingKey, ctable)
	if err != nil {
		log.Println(err)
		return "", err

	}
	defer ch.Close()

	table := amqp.Table{
		"request-id": requestid,
	}

	err = rabbitmq.PublishMessage(publisher, Exchange, RoutingKey, table, []byte(url))
	if err != nil {
		log.Println("PublishMessage", err)

		return "", err
	}

	var miniourl string

	notforever := make(chan bool)

	go func() {
		for d := range msgs {
			miniourl = string(d.Body)
			notforever <- true
		}
	}()
	<-notforever

	return miniourl, nil
}
