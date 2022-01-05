package main

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
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

func main() {
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

	// 声明交换机
	err = ch.ExchangeDeclare(
		"logs-exchange", // name
		"fanout",        // type
		true,            // durable
		false,           // auto-deleted
		false,           // internal
		false,           // no-wait
		nil,             // arguments
	)
	failOnError(err, "Failed to declare a exchange")

	for i := 0; i < 10; i++ {
		// 消息内容
		body := "this is new log message."
		// 发布消息导致的交换机
		err = ch.Publish(
			"logs-exchange", // exchange
			"",              // routing key
			false,           // mandatory
			false,           // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(body),
			})
		failOnError(err, "Failed to publish a message")

		log.Printf(" [Producer(pub/sub)] Sent %s", body)
	}
}
