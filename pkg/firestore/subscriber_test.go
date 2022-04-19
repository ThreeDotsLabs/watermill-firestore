package firestore_test

import (
	"context"
	"testing"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/stretchr/testify/require"
)

func TestSubscriber_picks_messages_from_subscription_that_were_sent_before_actual_subscribing(t *testing.T) {
	pub, sub := createPubSub(t)
	topic := "topic_" + watermill.NewShortUUID()

	err := sub.(message.SubscribeInitializer).SubscribeInitialize(topic)
	require.NoError(t, err)

	msg := message.NewMessage(watermill.NewUUID(), message.Payload{})
	err = pub.Publish(topic, msg)
	require.NoError(t, err)

	msgs, err := sub.Subscribe(context.Background(), topic)
	require.NoError(t, err)

	receivedMsg := <-msgs
	receivedMsg.Ack()
	require.Equal(t, receivedMsg.UUID, msg.UUID)
}
