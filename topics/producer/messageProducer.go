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
		"topic_demo", // name
		"topic",      // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	failOnError(err, "Failed to declare a exchange")

	// 发布消息到指定减环节并指定routing key
	var routing_key = [5]string{"aaa.bbb.ccc", "ccc.bbb.aaa", "aaa.aaa.aaa", "aaa.yyy.zzz", "xxx.yyy.zzz"}
	for i := range routing_key {
		// 消息内容
		body := "this is new " + routing_key[i] + " message."
		// 发布消息到指定的交换机并指定routing key
		err = ch.Publish(
			"topic_demo",   // exchange
			routing_key[i], // routing key
			false,          // mandatory
			false,          // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(body),
			})
		failOnError(err, "Failed to publish a message")

		log.Printf(" [Producer(routing)] Sent %s", body)
	}
}
