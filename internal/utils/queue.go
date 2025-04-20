package utils

import (
	"bytes"
	"encoding/csv"
	"iter"

	amqp "github.com/rabbitmq/amqp091-go"
)

type ConsumerQueue struct {
	ch                *amqp.Channel
	queueName         string
	consumerInstances int
	deliveryChannel   <-chan amqp.Delivery
}

func NewConsumerQueue(conn *amqp.Connection, queueName string, exchangeName string) (*ConsumerQueue, error) {
	return NewConsumerQueueWithRoutingKey(conn, queueName, exchangeName, queueName)
}

func NewConsumerQueueWithRoutingKey(conn *amqp.Connection, queueName string, exchangeName string, routingKey string) (*ConsumerQueue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	err = ch.ExchangeDeclare(
		exchangeName, // name
		"direct",     // type.  Consider making this configurable in NewQueue if needed
		false,        // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	if err != nil {
		return nil, err
	}

	_, err = ch.QueueDeclare(queueName, false, false, false, false, nil)
	if err != nil {
		return nil, err
	}

	err = ch.QueueBind(
		queueName,
		routingKey,
		exchangeName,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	deliveryChannel, err := ch.Consume(
		queueName, // queue name - use the stored queue name
		"",        // id
		false,     // auto-ack
		false,     // exclusive
		false,     // no-local
		false,     // no-wait
		nil,       // args
	)
	if err != nil {
		return nil, err
	}

	return &ConsumerQueue{ch: ch, queueName: queueName, consumerInstances: 0, deliveryChannel: deliveryChannel}, nil
}

func (q *ConsumerQueue) Consume() iter.Seq[*amqp.Delivery] {
	return func(yield func(*amqp.Delivery) bool) {
		for delivery := range q.deliveryChannel {
			if !yield(&delivery) {
				return
			}
		}
	}
}

func EncodeArrayToCsv(arr []string) string {
	buf := bytes.NewBuffer(nil)
	writer := csv.NewWriter(buf)

	writer.Write(arr)
	writer.Flush()

	return buf.String()
}

func (q *ConsumerQueue) CloseChannel() error {
	return q.ch.Close()
}

type ProducerQueue struct {
	ch           *amqp.Channel
	QueueName    string
	exchangeName string
}

func NewProducerQueue(conn *amqp.Connection, queueName string, exchangeName string) (*ProducerQueue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	err = ch.ExchangeDeclare(
		exchangeName, // name
		"direct",     // type.  Consider making this configurable in NewQueue if needed
		false,        // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	if err != nil {
		return nil, err
	}

	return &ProducerQueue{ch: ch, QueueName: queueName, exchangeName: exchangeName}, nil
}

func (q *ProducerQueue) Publish(body []byte) error {
	return q.PublishWithRoutingKey(body, q.QueueName)
}

func (q *ProducerQueue) PublishWithRoutingKey(body []byte, routingKey string) error {
	err := q.ch.Publish(
		q.exchangeName, // exchange
		routingKey,     // routing key
		false,          // mandatory
		false,          // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        body,
		})
	if err != nil {
		return err
	}
	return nil
}

func (q *ProducerQueue) CloseChannel() error {
	return q.ch.Close()
}
