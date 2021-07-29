package main

import (
	"context"
	"os"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-firestore/pkg/firestore"
	"github.com/ThreeDotsLabs/watermill/message"
)

func main() {
	logger := watermill.NewStdLogger(true, false)

	subscriber, err := firestore.NewSubscriber(
		firestore.SubscriberConfig{
			GenerateSubscriptionName: func(topic string) string {
				return topic + "_sub"
			},
			ProjectID: os.Getenv("FIRESTORE_PROJECT_ID"),
		},
		logger,
	)
	if err != nil {
		panic(err)
	}

	output1, err := subscriber.Subscribe(context.Background(), "topic")
	if err != nil {
		panic(err)
	}

	output2, err := subscriber.Subscribe(context.Background(), "topic")
	if err != nil {
		panic(err)
	}

	go read(output1)
	go read(output2)

	publisher, err := firestore.NewPublisher(
		firestore.PublisherConfig{
			ProjectID: os.Getenv("FIRESTORE_PROJECT_ID"),
		},
		logger,
	)
	if err != nil {
		panic(err)
	}

	go publish(publisher)

	<-time.After(time.Second * 20)
	subscriber.Close()
}

func publish(p *firestore.Publisher) {
	for {
		err := p.Publish("topic", message.NewMessage(watermill.NewShortUUID(), []byte("test")))
		if err != nil {
			panic(err)
		}
		<-time.After(2 * time.Second)
	}
}

func read(output <-chan *message.Message) {
	for msg := range output {
		if msg == nil {
			return
		}
		<-time.After(time.Second)
		msg.Ack()
	}
}
