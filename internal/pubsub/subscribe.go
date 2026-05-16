package pubsub

import (
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int

const (
	Durable SimpleQueueType = iota
	Transient
)

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // SimpleQueueType is an "enum" type I made to represent "durable" or "transient"
) (*amqp.Channel, amqp.Queue, error) {

	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("error creating channel: %w", err)
	}

	q, err := ch.QueueDeclare(
		queueName,
		queueType == Durable,
		queueType == Transient,
		queueType == Transient, // exclusive
		false,                  // noWait
		nil,                    // args
	)

	ch.QueueBind(
		q.Name,
		key,
		exchange,
		false, // noWait
		nil,   // args
	)

	return ch, q, err
}

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T),
) error {

	ch, q, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return fmt.Errorf("error declaring and binding queue: %w", err)
	}

	deliveryCh, err := ch.Consume(
		q.Name,
		"",    // consumer
		false, // autoAck
		false, // exclusive
		false, // noLocal
		false, // noWait
		nil,   // args
	)
	if err != nil {
		return fmt.Errorf("error consuming from queue: %w", err)
	}

	go func() {
		for d := range deliveryCh {
			var msg T
			err := json.Unmarshal(d.Body, &msg)
			if err != nil {
				fmt.Printf("Error unmarshaling message: %v\n", err)
				continue
			}
			handler(msg)
			d.Ack(false)
		}
	}()
	return nil
}
