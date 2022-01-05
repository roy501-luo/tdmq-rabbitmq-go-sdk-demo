package main

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"math/rand"
	"strconv"
	"time"
)

const (
	// 连接地址从控制台获取
	host = "amqp-xx.rabbitmq.xx.tencenttdmq.com"
	// 角色名称, 位于【角色管理】页面
	username = "admin"
	// 角色密钥, 位于【角色管理】页面
	password = "eyJrZXl..."
	// 要使用vhost全称
	vhost = "amqp-...|Vhost"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func randomString(l int) string {
	bytes := make([]byte, l)
	for i := 0; i < l; i++ {
		bytes[i] = byte(randInt(65, 90))
	}
	return string(bytes)
}

func randInt(min int, max int) int {
	return min + rand.Intn(max-min)
}

func fibonacciRPC(n int) (res int, err error) {
	// 创建连接
	conn, err := amqp.Dial("amqp://" + username + ":" + password + "@" + host + ":5672/" + vhost)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer func(conn *amqp.Connection) {
		err := conn.Close()
		if err != nil {

		}
	}(conn)

	// 建立通道
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer func(ch *amqp.Channel) {
		err := ch.Close()
		if err != nil {

		}
	}(ch)

	// 声明回调队列
	q, err := ch.QueueDeclare(
		"rpc-reply-queue", // name
		false,             // durable
		false,             // delete when unused
		true,              // exclusive
		false,             // noWait
		nil,               // arguments
	)
	failOnError(err, "Failed to declare a queue")

	// 创建消费者处理RPC回调信息
	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	corrId := randomString(32)

	// 发布消息到指定消息队列(发起RPC请求)
	err = ch.Publish(
		"",          // exchange
		"rpc_queue", // routing key
		false,       // mandatory
		false,       // immediate
		amqp.Publishing{
			ContentType:   "text/plain",
			CorrelationId: corrId,
			ReplyTo:       q.Name,
			Body:          []byte(strconv.Itoa(n)),
		})
	failOnError(err, "Failed to publish a message")

	for d := range msgs {
		// 验证响应与请求的关系
		if corrId == d.CorrelationId {
			res, err = strconv.Atoi(string(d.Body))
			failOnError(err, "Failed to convert body to integer")
			break
		}
	}

	return
}

func main() {
	rand.Seed(time.Now().UTC().UnixNano())

	n := 6

	log.Printf(" [RPC Client] Requesting fib(%d)", n)
	res, err := fibonacciRPC(n)
	failOnError(err, "Failed to handle RPC request")

	log.Printf(" [RPC Client] Get RPC response %d", res)
}
