package main

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
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

func main() {
	// 创建连接
	conn, err := amqp.Dial("amqp://" + username + ":" + password + "@" + host + ":5672/" + vhost)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer func(conn *amqp.Connection) {
		err := conn.Close()
		if err != nil {

		}
	}(conn)

	// 简历通道
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
	failOnError(err, "Failed to declare an exchange")

	// 声明消息队列
	q, err := ch.QueueDeclare(
		"fanout-consumer2", // name
		false,              // durable
		false,              // delete when unused
		true,               // exclusive
		false,              // no-wait
		nil,                // arguments
	)
	failOnError(err, "Failed to declare a queue")

	// 绑定消息队列到指定交换机
	err = ch.QueueBind(
		q.Name,          // queue name
		"",              // routing key
		"logs-exchange", // exchange
		false,
		nil,
	)
	failOnError(err, "Failed to bind a queue")

	// 设置每次投递一个消息
	err = ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	failOnError(err, "Failed to set QoS")

	// 创建消费者并消费指定消息队列中的消息
	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // 设置为不自动确认(可根据需求自己选择)
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	// 持续获取消息队列中的消息
	forever := make(chan bool)

	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s", d.Body)
			t := time.Duration(2)
			time.Sleep(t * time.Second)
			// 手动回复ack
			d.Ack(false)
		}
	}()

	log.Printf(" [Consumer2(pub/sub)] Waiting for messages.")
	<-forever
}
