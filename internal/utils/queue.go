package utils

import (
	"fmt"
	"iter"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
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
	isLeader           map[string]bool
	replicas           int
	finishSubscribers  []FinishSubscriber
	signalChan         chan os.Signal
	replicasMap        map[string]int
}

func NewConsumerQueue(conn *amqp.Connection, queueName string, exchangeName string, fanoutName string) (*ConsumerQueue, error) {
	return NewConsumerQueueWithRoutingKey(conn, queueName, exchangeName, queueName, fanoutName)
}

func getQueueName(queueName string, routingKey string) string {
	if routingKey == queueName {
		return queueName
	}
	return fmt.Sprintf("%s_%s", queueName, routingKey)
}

func NewConsumerQueueWithRoutingKey(conn *amqp.Connection, queueName string, exchangeName string, routingKey string, fanoutName string) (*ConsumerQueue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	replicas := os.Getenv("REPLICAS")
	if replicas == "" {
		log.Printf("REPLICAS environment variable not set, defaulting to 1")
		replicas = "1"
	}
	replicasInt, err := strconv.Atoi(replicas)
	if err != nil {
		return nil, fmt.Errorf("failed to convert replicas to int: %v", err)
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

	uniqueQueueName := getQueueName(queueName, routingKey)
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

	replicasMap := make(map[string]int)
	isLeader := make(map[string]bool)

	if fanoutName != "" {
		closeQueueConsumer, err := NewConsumerFanout(conn, fanoutName)
		if err != nil {
			return nil, err
		}
		closeQueueProducer, err := NewProducerFanout(conn, fanoutName)
		if err != nil {
			return nil, err
		}

		return &ConsumerQueue{ch: ch, queueName: uniqueQueueName, closeQueueConsumer: closeQueueConsumer, closeQueueProducer: closeQueueProducer, deliveryChannel: deliveryChannel, fanoutName: fanoutName, signalChan: signalChan, replicas: replicasInt, replicasMap: replicasMap, isLeader: isLeader}, nil
	}

	return &ConsumerQueue{ch: ch, queueName: uniqueQueueName, deliveryChannel: deliveryChannel, fanoutName: fanoutName, signalChan: signalChan, replicas: replicasInt, replicasMap: replicasMap, isLeader: isLeader}, nil
}

func (q *ConsumerQueue) ConsumeSink() iter.Seq[*amqp.Delivery] {
	return func(yield func(*amqp.Delivery) bool) {
		for {
			select {
			case <-q.signalChan:
				log.Printf("Received SIGTERM signal, closing connection")
				return
			case delivery := <-q.deliveryChannel:
				if !yield(&delivery) {
					log.Printf("Exiting early consumer loop")
					return
				}
			}
		}
	}
}

func (q *ConsumerQueue) consume(infinite bool) iter.Seq[*amqp.Delivery] {
	return func(yield func(*amqp.Delivery) bool) {

		var closeQueueDelivery <-chan amqp.Delivery
		if q.fanoutName != "" {
			closeQueueDelivery = q.closeQueueConsumer.deliveryChannel
		}

		for {
			select {
			case <-q.signalChan:
				log.Printf("Received SIGTERM signal, closing connection")
				return
			case delivery := <-closeQueueDelivery: // FINISHED-RECEIVED | FINISHED-DONE
				delivery.Ack(false)

				message := string(delivery.Body)
				clientId := delivery.Headers["clientId"].(string)

				if message == "FINISHED-RECEIVED" {
					log.Printf("Received FINISHED-RECEIVED")
					q.closeQueueProducer.Publish([]byte("FINISHED-DONE"), clientId)
					log.Printf("Sent FINISHED-DONE to %s", q.closeQueueProducer.queueName)
					if !infinite && !q.isLeader[clientId] {
						return
					}
				}

				// if message == "FINISHED-ACK" && q.isLeader {
				// 	log.Printf("Received FINISHED-ACK")
				// 	q.replicas++
				// 	log.Printf("Replicas counted: %d", q.replicas)
				// }

				if message == "FINISHED-DONE" && q.isLeader[clientId] {
					log.Printf("Received FINISHED-DONE")
					q.replicasMap[clientId]--
					log.Printf("Replicas remaining: %d", q.replicas)
					if q.replicasMap[clientId] == 0 {
						log.Println("All replicas finished for clientId ", clientId)
						q.sendFinished(clientId)
						if !infinite {
							return
						}
					}
				}
			case delivery := <-q.deliveryChannel:
				// if q.timer != nil {
				// 	q.timer = time.After(time.Millisecond * 1500)
				// }

				clientId := delivery.Headers["clientId"].(string)

				if _, ok := q.replicasMap[clientId]; !ok {
					q.replicasMap[clientId] = q.replicas
				}

				if string(delivery.Body) == "FINISHED" && q.fanoutName != "" {
					log.Printf("Received FINISHED from %s", q.queueName)
					q.isLeader[clientId] = true
					log.Printf("Is leader: %t", q.isLeader[clientId])
					q.closeQueueProducer.Publish([]byte("FINISHED-RECEIVED"), clientId)
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
				// case <-q.timer:
				// 	q.closeQueueProducer.Publish([]byte("FINISHED-DONE"), "AAAA")
				// 	log.Printf("Sent FINISHED-DONE")
				// 	if !q.isLeader {
				// 		return
				// 	}
			}
		}
	}
}

func (q *ConsumerQueue) ConsumeInfinite() iter.Seq[*amqp.Delivery] {
	return q.consume(true)
}

func (q *ConsumerQueue) Consume() iter.Seq[*amqp.Delivery] {
	return q.consume(false)
}

func (q *ConsumerQueue) AddFinishSubscriber(pq *ProducerQueue) {
	q.finishSubscribers = append(q.finishSubscribers, FinishSubscriber{producer: pq, routingKey: ""})
}
func (q *ConsumerQueue) AddFinishSubscriberWithRoutingKey(pq *ProducerQueue, routingKey string) {
	q.finishSubscribers = append(q.finishSubscribers, FinishSubscriber{producer: pq, routingKey: routingKey})
}

func (q *ConsumerQueue) sendFinished(clientId string) {
	if !q.isLeader[clientId] {
		return
	}
	for _, sub := range q.finishSubscribers {
		log.Printf("Sending FINISHED to %s", sub.producer.queueName)
		if sub.routingKey == "" {
			sub.producer.Publish([]byte("FINISHED"), clientId)
		} else {
			sub.producer.PublishWithRoutingKey([]byte("FINISHED"), sub.routingKey, clientId)
		}
	}
}

func (q *ConsumerQueue) CloseChannel() error {
	return q.ch.Close()
}

type ProducerQueue struct {
	ch           *amqp.Channel
	boundQueues  map[string]bool
	queueName    string
	exchangeName string
	isFanout     bool
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

	return &ProducerQueue{ch: ch, queueName: queueName, exchangeName: exchangeName, boundQueues: make(map[string]bool), isFanout: false}, nil
}

func (q *ProducerQueue) Publish(body []byte, clientId string) error {
	return q.PublishWithRoutingKey(body, q.queueName, clientId)
}

func (q *ProducerQueue) PublishWithRoutingKey(body []byte, routingKey string, clientId string) error {
	if !q.boundQueues[routingKey] && !q.isFanout {
		name := getQueueName(q.queueName, routingKey)
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

	replicasMap := make(map[string]int)
	isLeader := make(map[string]bool)

	return &ConsumerQueue{ch: ch, queueName: q.Name, deliveryChannel: deliveryChannel, replicasMap: replicasMap, isLeader: isLeader}, nil
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

	return &ProducerQueue{ch: ch, exchangeName: exchangeName, isFanout: true}, nil
}
