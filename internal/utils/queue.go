package utils

import (
	"iter"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type ConsumerQueue struct {
	ch                 *amqp.Channel
	queueName          string
	deliveryChannel    <-chan amqp.Delivery
	fanoutName         string
	closeQueueConsumer *ConsumerQueue
	closeQueueProducer *ProducerQueue
	timer              <-chan time.Time
	isLeader           bool
	replicas           int
}

func NewConsumerQueue(conn *amqp.Connection, queueName string, exchangeName string, fanoutName string) (*ConsumerQueue, error) {
	return NewConsumerQueueWithRoutingKey(conn, queueName, exchangeName, queueName, fanoutName)
}

func NewConsumerQueueWithRoutingKey(conn *amqp.Connection, queueName string, exchangeName string, routingKey string, fanoutName string) (*ConsumerQueue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	err = ch.ExchangeDeclare(
		exchangeName, // name
		"direct",     // type.
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

	if fanoutName != "" {
		closeQueueConsumer, err := NewConsumerFanout(conn, fanoutName)
		if err != nil {
			return nil, err
		}
		closeQueueProducer, err := NewProducerFanout(conn, fanoutName)
		if err != nil {
			return nil, err
		}

		return &ConsumerQueue{ch: ch, queueName: queueName, closeQueueConsumer: closeQueueConsumer, closeQueueProducer: closeQueueProducer, deliveryChannel: deliveryChannel, fanoutName: fanoutName}, nil
	}

	return &ConsumerQueue{ch: ch, queueName: queueName, deliveryChannel: deliveryChannel, fanoutName: fanoutName}, nil
}

func (q *ConsumerQueue) Consume() iter.Seq[*amqp.Delivery] {
	return func(yield func(*amqp.Delivery) bool) {

		var closeQueueDelivery <-chan amqp.Delivery
		if q.fanoutName != "" {
			closeQueueDelivery = q.closeQueueConsumer.deliveryChannel
		}

		for {
			select {
			case delivery := <-closeQueueDelivery: // FINISHED-RECEIVED | FINISHED-DONE | FINISHED-ACK
				message := string(delivery.Body)
				if message == "FINISHED-RECEIVED" {
					q.timer = time.After(time.Millisecond * 1000)
					q.closeQueueProducer.Publish([]byte("FINISHED-ACK"))
				}

				if message == "FINISHED-ACK" && q.isLeader {
					q.replicas++
				}
				if message == "FINISHED-DONE" && q.isLeader {

					q.replicas--
					if q.replicas == 0 {
						q.closeQueueProducer.Publish([]byte("FINISHED-DONE"))
					}
				}
			case delivery := <-q.deliveryChannel:
				if q.timer != nil {
					q.timer = time.After(time.Millisecond * 1000)
				}
				if string(delivery.Body) == "FINISHED" && q.fanoutName != "" {
					q.isLeader = true
					q.closeQueueProducer.Publish([]byte("FINISHED-RECEIVED"))
					delivery.Ack(false)
					continue
				}
				if !yield(&delivery) {
					return
				}
			case <-q.timer:
				q.closeQueueProducer.Publish([]byte("FINISHED-DONE"))
				return
			}
		}
	}
}

func (q *ConsumerQueue) CloseChannel() error {
	return q.ch.Close()
}

type ProducerQueue struct {
	ch           *amqp.Channel
	queueName    string
	exchangeName string
}

func NewProducerQueue(conn *amqp.Connection, queueName string, exchangeName string) (*ProducerQueue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	err = ch.ExchangeDeclare(
		exchangeName, // name
		"direct",     // type.
		false,        // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	if err != nil {
		return nil, err
	}

	return &ProducerQueue{ch: ch, queueName: queueName, exchangeName: exchangeName}, nil
}

func (q *ProducerQueue) Publish(body []byte) error {
	return q.PublishWithRoutingKey(body, q.queueName)
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

func NewConsumerFanout(conn *amqp.Connection, exchangeName string) (*ConsumerQueue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	err = ch.ExchangeDeclare(
		exchangeName, // name
		"fanout",     // type.
		false,        // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	if err != nil {
		return nil, err
	}

	q, err := ch.QueueDeclare("", false, false, false, false, nil)
	if err != nil {
		return nil, err
	}

	err = ch.QueueBind(
		q.Name,
		"",
		exchangeName,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	deliveryChannel, err := ch.Consume(
		q.Name, // queue name - use the stored queue name
		"",     // id
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		return nil, err
	}

	return &ConsumerQueue{ch: ch, queueName: q.Name, deliveryChannel: deliveryChannel}, nil
}

func NewProducerFanout(conn *amqp.Connection, exchangeName string) (*ProducerQueue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	err = ch.ExchangeDeclare(
		exchangeName, // name
		"fanout",     // type.
		false,        // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	if err != nil {
		return nil, err
	}

	return &ProducerQueue{ch: ch, exchangeName: exchangeName}, nil
}
