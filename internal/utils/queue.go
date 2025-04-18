package utils

import (
	"bytes"
	"context"
	"encoding/csv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Queue struct {
	ch *amqp.Channel
	q  amqp.Queue
}

func NewQueue(conn *amqp.Connection, name string, durable bool, autoDelete bool, exclusive bool, noWait bool, args amqp.Table) (*Queue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}
	q, err := ch.QueueDeclare(name, durable, autoDelete, exclusive, noWait, args)
	if err != nil {
		return nil, err
	}

	return &Queue{ch: ch, q: q}, nil
}

func (q *Queue) Publish(body []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := q.ch.PublishWithContext(ctx, "", q.q.Name, false, false, amqp.Publishing{
		ContentType: "text/plain",
		Body:        body,
	})
	if err != nil {
		return err
	}

	return nil
}

func (q *Queue) Consume() (<-chan amqp.Delivery, error) {
	msgs, err := q.ch.Consume(q.q.Name, "", true, false, false, false, nil)
	if err != nil {
		return nil, err
	}
	return msgs, nil
}

func EncodeArrayToCsv(arr []string) string {
	buf := bytes.NewBuffer(nil)
	writer := csv.NewWriter(buf)

	writer.Write(arr)
	writer.Flush()

	return buf.String()
}
