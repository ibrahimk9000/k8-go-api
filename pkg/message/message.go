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

	publisher, err := rabbitmq.NewQueuePublisher(conn, Exchange)
	if err != nil {
		log.Println("%s", err)
		return "", err

	}

	defer publisher.Close()

	// Start a consumer
	msgs, ch, err := rabbitmq.NewQueueConsumer(conn, AqueueName, Aexchange, AroutingKey)
	if err != nil {
		log.Println("%s", err)
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
			log.Printf("Received a message: %s", d.Body)
			miniourl = string(d.Body)
			notforever <- true
		}
	}()
	<-notforever

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")

	return miniourl, nil
}
