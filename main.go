package main

import (
	"flag"
	"fmt"
	//"os"
	"time"

	"github.com/BurntSushi/toml"
	log "github.com/Sirupsen/logrus"
	"github.com/streadway/amqp"
)

func main() {
	log.SetLevel(log.DebugLevel)

	config := getConfig()
	InitConnection(config.ConnectionCount, config.RabbitMQ_URI)
	InitChannel(config.ChannelCount)
	InitExchange(config.ExchangeCount)
	log.Debugf("Consumer starting (%d)", config.ConsumerCount)
	for i := 0; i < config.ConsumerCount; i++ {
		time.Sleep(time.Millisecond * 20)
		go MakeConsumer()
	}
	log.Debugf("Consumer created (%d)", config.ConsumerCount)
	log.Debugf("Start sending message")
	for {
		SendMsg("message")
	}

}

type Config struct {
	RabbitMQ_URI    string
	ConsumerCount   int
	ConnectionCount int
	ChannelCount    int
	ExchangeCount   int
}

var connections []*amqp.Connection
var channels []*amqp.Channel

func InitConnection(count int, RabbitMQ_URI string) {
	log.Debugf("Connections starting (%d)", count)
	connections = make([]*amqp.Connection, count, count)
	for i := 0; i < count; i++ {
		connection, err := amqp.Dial(RabbitMQ_URI)
		failOnError(err, "Init connection")
		connections[i] = connection
	}
	log.Debugf("Connections Created (%d)", count)
}

func InitChannel(count int) {
	log.Debugf("Channels starting (%d)", count)
	channels = make([]*amqp.Channel, count, count)
	for i := 0; i < count; i++ {
		ch, err := GetConnection().Channel()
		failOnError(err, "Init channel")
		channels[i] = ch
	}
	log.Debugf("Channels Created (%d)", count)
}

func InitExchange(count int) {
	log.Debugf("Exchange starting (%d)", count)
	maxExchange = count
	for i := 0; i < count; i++ {
		ch := GetChannel()
		err := ch.ExchangeDeclare(
			fmt.Sprintf("rabbit-benchmark-%d", i), // name
			"fanout", // type
			true,     // durable
			false,    // auto-deleted
			false,    // internal
			false,    // no-wait
			nil,      // arguments
		)
		failOnError(err, "exchange declare")
	}
	log.Debugf("Exchange Created (%d)", count)
}

var currentExchange = -1
var maxExchange = 0

func GetExchange() string {
	currentExchange++
	if currentExchange >= maxExchange {
		currentExchange = 0
	}
	return fmt.Sprintf("rabbit-benchmark-%d", currentExchange)
}

var currentConnectionIndex = -1

func GetConnection() *amqp.Connection {
	currentConnectionIndex++
	if currentConnectionIndex >= len(connections) {
		currentConnectionIndex = 0
	}
	return connections[currentConnectionIndex]
}

var currentChannelIndex = -1

func GetChannel() *amqp.Channel {
	currentChannelIndex++
	if currentChannelIndex >= len(channels) {
		currentChannelIndex = 0
	}
	return channels[currentConnectionIndex]
}

func failOnError(err error, prefix string) {
	if err != nil {
		log.Errorf("Err %s: %s", prefix, err)
		//os.Exit(1)
	}
}

func getConfig() Config {
	configFile := flag.String("config", "config.toml", "`config file`")
	flag.Parse()

	log.Debugf("Config file: %s", *configFile)
	var config = Config{}
	_, err := toml.DecodeFile(*configFile, &config)
	failOnError(err, "get config")
	return config
}

func SendMsg(body string) {
	ch := GetChannel()
	err := ch.Publish(
		GetExchange(), // exchange
		"",            // routing key
		false,         // mandatory
		false,         // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		})
	failOnError(err, "publish")
}

func MakeConsumer() {

	ch := GetChannel()

	q, err := ch.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when usused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	failOnError(err, "queue declare")

	//log.Debugf("%s", q)
	err = ch.QueueBind(
		q.Name,        // queue name
		"",            // routing key
		GetExchange(), // exchange
		false,
		nil)
	failOnError(err, "queue bind")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "consume")
	for range msgs {
		//log.Printf(" [x] %s", d.Body)
	}
	log.Debugf("bye bye consumer")
}
