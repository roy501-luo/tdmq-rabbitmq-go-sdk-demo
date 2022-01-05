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
	// 交换机名称
	exchangeName = "delay_exchange"
	// 队列名称
	queueName = "delay-queue"
	// 路由键
	routingKey = "delay"
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

	argsQue := make(map[string]interface{})
	//添加过期时间
	argsQue["x-delayed-type"] = "direct"

	// 声明交换机
	err = ch.ExchangeDeclare(
		exchangeName,        // name
		"x-delayed-message", // type
		true,                // durable
		false,               // auto-deleted
		false,               // internal
		false,               // no-wait
		argsQue,             // arguments
	)

	// 声明消息队列
	queue, err := ch.QueueDeclare(
		queueName,
		false,
		false,
		false,
		false,
		nil)
	failOnError(err, "Failed to declare a queue")

	// 绑定消息队列到指定交换机并订阅对应的 routing key
	err = ch.QueueBind(
		queue.Name,       // queue name
		routingKey,       // routing key
		"delay_exchange", // exchange
		false,
		nil,
	)

	// 消息内容
	body := "Hello world"

	// 发布消息到指定的消息队列中
	err = ch.Publish(
		exchangeName, // exchange
		routingKey,   // routing key
		false,        // mandatory
		false,        // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Headers:     map[string]interface{}{"x-delay": 4000}, //单位毫秒
			Body:        []byte(body),
		})
	failOnError(err, "Failed to publish a message")

	log.Printf(" [Producer(Hello world)] Sent %s", body)
}
