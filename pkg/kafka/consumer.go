package kafka

import (
	"context"
	"errors"
	"fmt"

	kafkago "github.com/segmentio/kafka-go"
)

type Message struct {
	Key       []byte
	Value     []byte
	raw       kafkago.Message
	Topic     string
	Partition int
	Offset    int64
}

type Consumer struct {
	reader *kafkago.Reader
}

func NewConsumer(topic string, groupID string, brokers []string) *Consumer {
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Topic:   topic,
		GroupID: groupID,
		Brokers: brokers,
	})

	return &Consumer{
		reader: reader,
	}
}

func (c *Consumer) Fetch(ctx context.Context) (Message, error) {
	msg, err := c.reader.FetchMessage(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return Message{}, err
		}

		return Message{}, fmt.Errorf("failed to fetch message: %w", err)
	}

	return Message{
		Key:       msg.Key,
		Value:     msg.Value,
		raw:       msg,
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Offset:    msg.Offset,
	}, nil
}

func (c *Consumer) Commit(ctx context.Context, msg Message) error {
	return c.reader.CommitMessages(ctx, msg.raw)
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}
