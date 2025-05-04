package utils

import (
	"fmt"
	"iter"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/MateoVroonland/tp-distro/internal/env"
	amqp "github.com/rabbitmq/amqp091-go"
)

type ConsumerQueue struct {
	ch               *amqp.Channel
	queueName        string
	deliveryChannel  <-chan amqp.Delivery
	signalChan       chan os.Signal
	previousReplicas int
	finishedReceived map[string]map[string]bool
}

func NewConsumerQueue(conn *amqp.Connection, queueName string, exchangeName string, previousReplicas int) (*ConsumerQueue, error) {
	id := env.AppEnv.ID
	return newConsumerQueueWithRoutingKey(conn, queueName, exchangeName, strconv.Itoa(id), previousReplicas)
}

func newConsumerQueueWithRoutingKey(conn *amqp.Connection, queueName string, exchangeName string, routingKey string, previousReplicas int) (*ConsumerQueue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGTERM)

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

	uniqueQueueName := fmt.Sprintf("%s_%s", queueName, routingKey)
	_, err = ch.QueueDeclare(uniqueQueueName, false, false, false, false, nil)
	if err != nil {
		return nil, err
	}

	err = ch.QueueBind(
		uniqueQueueName,
		routingKey,
		exchangeName,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	deliveryChannel, err := ch.Consume(
		uniqueQueueName, // queue name - use the unique queue name
		"",              // id
		false,           // auto-ack
		false,           // exclusive
		false,           // no-local
		false,           // no-wait
		nil,             // args
	)
	if err != nil {
		return nil, err
	}

	return &ConsumerQueue{
		ch:               ch,
		queueName:        uniqueQueueName,
		deliveryChannel:  deliveryChannel,
		signalChan:       signalChan,
		finishedReceived: make(map[string]map[string]bool),
		previousReplicas: previousReplicas,
	}, nil
}

type Message struct {
	Body     string
	ClientId string
	delivery amqp.Delivery
}

func (m *Message) Ack() {
	m.delivery.Ack(false)
}

func (m *Message) Nack(requeue bool) {
	m.delivery.Nack(false, requeue)
}

func MessageFromDelivery(delivery amqp.Delivery) (*Message, error) {
	clientId, ok := delivery.Headers["clientId"].(string)
	if !ok {
		return nil, fmt.Errorf("clientId not found in headers")
	}

	return &Message{
		Body:     string(delivery.Body),
		ClientId: clientId,
		delivery: delivery,
	}, nil
}

func (q *ConsumerQueue) consume(infinite bool) iter.Seq[Message] {
	return func(yield func(Message) bool) {

		for {
			select {
			case <-q.signalChan:
				log.Printf("Received SIGTERM signal, closing connection")
				return
			case delivery := <-q.deliveryChannel:
				message, err := MessageFromDelivery(delivery)
				if err != nil {
					log.Printf("Failed to parse message in delivery channel: %v", err)
					log.Printf("delivery: %v", delivery)
					message.Nack(false)
					continue
				}

				if _, ok := q.finishedReceived[message.ClientId]; !ok {
					q.finishedReceived[message.ClientId] = make(map[string]bool)
				}
				if strings.HasPrefix(message.Body, "FINISHED:") {
					q.finishedReceived[message.ClientId][message.Body] = true
					if len(q.finishedReceived[message.ClientId]) == q.previousReplicas {
						log.Printf("Received all messages for client %s", message.ClientId)
						delete(q.finishedReceived, message.ClientId)
						if infinite {
							finishedMessage := message
							finishedMessage.Body = "FINISHED"
							if !yield(*finishedMessage) {
								log.Printf("Exiting early consumer loop")
								return
							}
							continue
						}
						message.Ack()
						return

					} else {
						message.Ack()
						continue
					}
				}

				if !yield(*message) {
					log.Printf("Exiting early consumer loop")
					return
				}
			}
		}
	}
}

// func (q *ConsumerQueue) ConsumeSink() iter.Seq[Message] {
// 	return q.consume(true, true)
// }

func (q *ConsumerQueue) ConsumeInfinite() iter.Seq[Message] {
	return q.consume(true)
}

func (q *ConsumerQueue) Consume() iter.Seq[Message] {
	return q.consume(false)
}

// func (q *ConsumerQueue) AddFinishSubscriber(pq *ProducerQueue) {
// 	q.finishSubscribers = append(q.finishSubscribers, FinishSubscriber{producer: pq, routingKey: ""})
// }
// func (q *ConsumerQueue) AddFinishSubscriberWithRoutingKey(pq *ProducerQueue, routingKey string) {
// 	q.finishSubscribers = append(q.finishSubscribers, FinishSubscriber{producer: pq, routingKey: routingKey})
// }

// func (q *ConsumerQueue) sendFinished(clientId string) {
// 	if !q.isLeader[clientId] {
// 		return
// 	}
// 	for _, sub := range q.finishSubscribers {
// 		log.Printf("Sending FINISHED to %s", sub.producer.queueName)
// 		if sub.routingKey == "" {
// 			sub.producer.Publish([]byte("FINISHED"), clientId)
// 		} else {
// 			sub.producer.PublishWithRoutingKey([]byte("FINISHED"), sub.routingKey, clientId)
// 		}
// 	}
// }

func (q *ConsumerQueue) CloseChannel() error {
	return q.ch.Close()
}

type ProducerQueue struct {
	ch           *amqp.Channel
	boundQueues  map[string]bool
	exchangeName string
	isFanout     bool
	nextReplicas int
}

func NewProducerQueue(conn *amqp.Connection, exchangeName string, nextReplicas int) (*ProducerQueue, error) {
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

	return &ProducerQueue{
		ch:           ch,
		exchangeName: exchangeName,
		boundQueues:  make(map[string]bool),
		isFanout:     false,
		nextReplicas: nextReplicas,
	}, nil
}

func (q *ProducerQueue) Publish(body []byte, clientId string, movieId string) error {
	routingKey := strconv.Itoa(HashString(movieId, q.nextReplicas))
	return q.publishWithRoutingKey(body, routingKey, clientId)
}

func (q *ProducerQueue) PublishFinished(clientId string) error {
	for i := range q.nextReplicas {
		log.Printf("Publishing FINISHED for client %s to exchange %s with routing key %s", clientId, q.exchangeName, strconv.Itoa(i+1))
		q.publishWithRoutingKey([]byte("FINISHED:"+strconv.Itoa(env.AppEnv.ID)), strconv.Itoa(i+1), clientId)
	}
	return nil
}

func (q *ProducerQueue) publishWithRoutingKey(body []byte, routingKey string, clientId string) error {
	if !q.boundQueues[routingKey] && !q.isFanout {
		name := fmt.Sprintf("%s_%s", q.exchangeName, routingKey)
		_, err := q.ch.QueueDeclare(name, false, false, false, false, nil)
		if err != nil {
			return err
		}
		err = q.ch.QueueBind(
			name,
			routingKey,
			q.exchangeName,
			false,
			nil,
		)
		if err != nil {
			return err
		}
		q.boundQueues[routingKey] = true
	}

	headers := amqp.Table{
		"clientId": clientId,
	}

	err := q.ch.Publish(
		q.exchangeName, // exchange
		routingKey,     // routing key
		true,           // mandatory
		false,          // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        body,
			Headers:     headers,
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
		q.Name, // queue name
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

	return &ConsumerQueue{
		ch:              ch,
		queueName:       q.Name,
		deliveryChannel: deliveryChannel,
		finishedReceived: make(map[string]map[string]bool),
	}, nil
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

	return &ProducerQueue{ch: ch, exchangeName: exchangeName, isFanout: true, nextReplicas: 1}, nil
}
