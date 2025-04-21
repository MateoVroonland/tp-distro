package utils

import (
	"fmt"
	"iter"
	"log"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type FinishSubscriber struct {
	producer   *ProducerQueue
	routingKey string
}

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
	finishSubscribers  []FinishSubscriber
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

	var uniqueQueueName string
	if routingKey == queueName {
		uniqueQueueName = queueName
	} else {
		uniqueQueueName = fmt.Sprintf("%s_%s", queueName, routingKey)
	}
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

	if fanoutName != "" {
		closeQueueConsumer, err := NewConsumerFanout(conn, fanoutName)
		if err != nil {
			return nil, err
		}
		closeQueueProducer, err := NewProducerFanout(conn, fanoutName)
		if err != nil {
			return nil, err
		}

		return &ConsumerQueue{ch: ch, queueName: uniqueQueueName, closeQueueConsumer: closeQueueConsumer, closeQueueProducer: closeQueueProducer, deliveryChannel: deliveryChannel, fanoutName: fanoutName}, nil
	}

	return &ConsumerQueue{ch: ch, queueName: uniqueQueueName, deliveryChannel: deliveryChannel, fanoutName: fanoutName}, nil
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
				delivery.Ack(false)

				message := string(delivery.Body)
				if message == "FINISHED-RECEIVED" {
					log.Printf("Received FINISHED-RECEIVED")
					q.timer = time.After(time.Millisecond * 1500)
					q.closeQueueProducer.Publish([]byte("FINISHED-ACK"))
					log.Printf("Sent FINISHED-ACK to %s", q.closeQueueProducer.queueName)
				}

				if message == "FINISHED-ACK" && q.isLeader {
					log.Printf("Received FINISHED-ACK")
					q.replicas++
					log.Printf("Replicas counted: %d", q.replicas)
				}
				if message == "FINISHED-DONE" && q.isLeader {
					log.Printf("Received FINISHED-DONE")
					q.replicas--
					log.Printf("Replicas remaining: %d", q.replicas)
					if q.replicas == 0 {
						q.sendFinished()
						return
					}
				}
			case delivery := <-q.deliveryChannel:
				if q.timer != nil {
					q.timer = time.After(time.Millisecond * 1500)
				}
				if string(delivery.Body) == "FINISHED" && q.fanoutName != "" {
					log.Printf("Received FINISHED from %s", q.queueName)
					q.isLeader = true
					log.Printf("Is leader: %t", q.isLeader)
					q.closeQueueProducer.Publish([]byte("FINISHED-RECEIVED"))
					log.Printf("Sent FINISHED-RECEIVED to")
					err := delivery.Ack(false)
					if err != nil {
						log.Printf("Failed to ack delivery: %v", err)
					}
					continue
				}
				if !yield(&delivery) {
					log.Printf("Exiting early consumer loop")
					return
				}
			case <-q.timer:
				q.closeQueueProducer.Publish([]byte("FINISHED-DONE"))
				log.Printf("Sent FINISHED-DONE")
				if !q.isLeader {
					return
				}
			}
		}
	}
}

func (q *ConsumerQueue) AddFinishSubscriber(pq *ProducerQueue) {
	q.finishSubscribers = append(q.finishSubscribers, FinishSubscriber{producer: pq, routingKey: ""})
}
func (q *ConsumerQueue) AddFinishSubscriberWithRoutingKey(pq *ProducerQueue, routingKey string) {
	q.finishSubscribers = append(q.finishSubscribers, FinishSubscriber{producer: pq, routingKey: routingKey})
}

func (q *ConsumerQueue) sendFinished() {
	if !q.isLeader {
		return
	}
	for _, sub := range q.finishSubscribers {
		log.Printf("Sending FINISHED to %s", sub.producer.queueName)
		if sub.routingKey == "" {
			sub.producer.Publish([]byte("FINISHED"))
		} else {
			sub.producer.PublishWithRoutingKey([]byte("FINISHED"), sub.routingKey)
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

	notifyReturn := make(chan amqp.Return)
	q.ch.NotifyReturn(notifyReturn)
	go func() {
		outputFile, err := os.OpenFile("returned_messages.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Printf("Failed to open output file: %v", err)
			outputFile = nil
		}
		outputFileInfo, err := os.OpenFile("returned_messages_info.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Printf("Failed to open output file: %v", err)
			outputFileInfo = nil
		}
		for r := range notifyReturn {
			body := string(r.Body)
			info := fmt.Sprintf("exchange: %s, routingKey: %s, replyText: %s\n", r.Exchange, r.RoutingKey, r.ReplyText)
			if outputFile != nil {
				fmt.Fprint(outputFile, body)
			}
			if outputFileInfo != nil {
				fmt.Fprint(outputFileInfo, info)
			}
		}
	}()

	err := q.ch.Publish(
		q.exchangeName, // exchange
		routingKey,     // routing key
		true,           // mandatory
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
