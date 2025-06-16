package utils

import (
	"fmt"
	"iter"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/MateoVroonland/tp-distro/internal/env"
	amqp "github.com/rabbitmq/amqp091-go"
)

type FanoutMessage struct {
	Body       string
	ClientId   string
	delivery   amqp.Delivery
	ProducerId string
}

func (m *FanoutMessage) Ack() {
	m.delivery.Ack(false)
}

func (m *FanoutMessage) Nack(requeue bool) {
	m.delivery.Nack(false, requeue)
}

func FanoutMessageFromDelivery(delivery amqp.Delivery) (*FanoutMessage, error) {
	clientId, ok := delivery.Headers["clientId"].(string)
	if !ok {
		return nil, fmt.Errorf("clientId not found in headers")
	}

	producerIdRaw, ok := delivery.Headers["producerId"]
	if !ok {
		return nil, fmt.Errorf("producerId not found in headers, headers: %s", delivery.Headers)
	}

	producerId, ok := producerIdRaw.(string)
	if !ok {
		return nil, fmt.Errorf("producerId of invalid type, producerIdRaw: %s", producerIdRaw)
	}

	return &FanoutMessage{
		Body:       string(delivery.Body),
		ClientId:   clientId,
		delivery:   delivery,
		ProducerId: producerId,
	}, nil
}

func (q *ConsumerFanout) Consume() iter.Seq[FanoutMessage] {
	return func(yield func(FanoutMessage) bool) {

		for {
			select {
			case <-q.signalChan:
				log.Printf("Received SIGTERM signal, closing connection")
				return
			case delivery := <-q.deliveryChannel:
				message, err := FanoutMessageFromDelivery(delivery)
				if err != nil {
					log.Printf("Failed to parse message in delivery channel: %v", err)
					log.Printf("delivery: %v", delivery)
					delivery.Nack(false, false)
					continue
				}

				if !yield(*message) {
					log.Printf("Exiting early consumer loop")
					return
				}
			}
		}
	}
}

type ProducerFanout struct {
	ch           *amqp.Channel
	boundQueues  map[string]bool
	exchangeName string
}

func NewProducerFanout(conn *amqp.Connection, exchangeName string) (*ProducerFanout, error) {
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

	return &ProducerFanout{
		ch:           ch,
		exchangeName: exchangeName,
		boundQueues:  make(map[string]bool),
	}, nil
}

func (q *ProducerFanout) Publish(body []byte, clientId string, routingKey string) error {
	return q.publishWithParams(body, routingKey, clientId, strconv.Itoa(env.AppEnv.ID))
}

func (q *ProducerFanout) publishWithParams(body []byte, routingKey string, clientId string, producerId string) error {
	if !q.boundQueues[routingKey] {
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
		"clientId":   clientId,
		"producerId": producerId,
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

func (q *ProducerFanout) CloseChannel() error {
	return q.ch.Close()
}

type ConsumerFanout struct {
	ch              *amqp.Channel
	queueName       string
	deliveryChannel <-chan amqp.Delivery
	signalChan      chan os.Signal
}

func NewConsumerFanout(conn *amqp.Connection, exchangeName string) (*ConsumerFanout, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGTERM)

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

	queueName := fmt.Sprintf("fanout_%s_%s", exchangeName, strconv.Itoa(env.AppEnv.ID))
	q, err := ch.QueueDeclare(queueName, false, false, false, false, nil)
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

	return &ConsumerFanout{
		ch:              ch,
		queueName:       q.Name,
		deliveryChannel: deliveryChannel,
		signalChan:      signalChan,
	}, nil
}

func (q *ConsumerFanout) DeleteQueue() error {
	_, err := q.ch.QueueDelete(q.queueName, false, false, false)
	log.Printf("Deleted queue %s", q.queueName)
	return err
}

func (q *ConsumerFanout) Close() error {
	return q.ch.Close()
}

func (q *ProducerFanout) Close() error {
	return q.ch.Close()
}
