package firestore

import (
	"context"
	"time"

	"cloud.google.com/go/firestore"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

type subscription struct {
	name   string
	topic  string
	config SubscriberConfig

	client client
	logger watermill.LoggerAdapter

	closing chan struct{}
	output  chan *message.Message
}

func newSubscription(
	name, topic string,
	config SubscriberConfig,
	client client,
	logger watermill.LoggerAdapter,
	closing chan struct{},
) (*subscription, error) {
	s := &subscription{
		name:    name,
		topic:   topic,
		config:  config,
		client:  client,
		logger:  logger,
		closing: closing,
		output:  make(chan *message.Message),
	}

	if err := createFirestoreSubscriptionIfNotExists(s.client, config.PubSubRootCollection, topic, s.config.GenerateSubscriptionName(topic), s.logger, s.config.Timeout); err != nil {
		return nil, err
	}

	return s, nil
}

func (s *subscription) receive(ctx context.Context) {
	s.readPendingMessages(ctx)

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return s.readPendingMessagesPeriodically(ctx)
	})
	g.Go(func() error {
		return s.watchIncomingMessages(ctx)
	})

	if err := g.Wait(); err != nil {
		s.logger.Error("Subscription receive failure", err, nil)
	}
}

func (s *subscription) readPendingMessagesPeriodically(ctx context.Context) error {
	ticker := time.NewTicker(s.config.ReadAllPeriod)

	for {
		select {
		case <-ticker.C:
			s.readPendingMessages(ctx)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *subscription) readPendingMessages(ctx context.Context) {
	docs, err := s.messagesQuery().Documents(ctx).GetAll()
	if err != nil {
		s.logger.Error("Couldn't read all messages from subscription", err, nil)
		return
	}
	for _, doc := range docs {
		s.handleAddedMessage(ctx, doc)
	}
}

func (s *subscription) watchIncomingMessages(ctx context.Context) error {
	subscriptionSnapshots := s.messagesQuery().Query.Snapshots(ctx)
	defer subscriptionSnapshots.Stop()
	for {
		subscriptionSnapshot, err := subscriptionSnapshots.Next()
		if err == iterator.Done {
			s.logger.Debug("Listening on subscription done", nil)
			return err
		} else if status.Code(err) == codes.Canceled {
			s.logger.Debug("Receive context canceled", nil)
			return err
		} else if err != nil {
			s.logger.Error("Error receiving", err, nil)
			return err
		}

		if subscriptionSnapshot.Size == 0 {
			continue
		}

		for _, e := range onlyAddedMessages(subscriptionSnapshot.Changes) {
			s.handleAddedMessage(ctx, e.Doc)
		}
	}
}

func (s *subscription) messagesQuery() *firestore.CollectionRef {
	return s.client.Collection(s.config.PubSubRootCollection).Doc(s.topic).Collection(s.name)
}

func onlyAddedMessages(changes []firestore.DocumentChange) (added []firestore.DocumentChange) {
	for _, ch := range changes {
		if ch.Kind == firestore.DocumentAdded {
			added = append(added, ch)
		}
	}
	return
}

func (s *subscription) handleAddedMessage(ctx context.Context, doc *firestore.DocumentSnapshot) {
	logger := s.logger.With(watermill.LogFields{"document_id": doc.Ref.ID})

	msg, err := s.config.Marshaler.Unmarshal(doc)
	if err != nil {
		logger.Error("Couldn't unmarshal message", err, nil)
		return
	}

	ctx, cancelCtx := context.WithCancel(ctx)
	msg.SetContext(ctx)
	defer cancelCtx()

	select {
	case <-s.closing:
		logger.Trace("Channel closed when waiting for consuming message", nil)
		return
	case <-ctx.Done():
		logger.Trace("Context done when waiting for consuming message", nil)
		return
	case s.output <- msg:
		logger.Trace("Message consumed, waiting for ack/nack", nil)
		// message consumed, wait for ack/nack
	}

	select {
	case <-s.closing:
		logger.Trace("Closing when waiting for ack/nack", nil)
	case <-msg.Nacked():
		logger.Trace("Message nacked", nil)
	case <-ctx.Done():
		logger.Trace("Context done", nil)
	case <-msg.Acked():
		if err := s.ackMessage(ctx, doc, logger); err != nil {
			logger.Error("Failed to ack message", err, nil)
			return
		}
		logger.Trace("Message acked", nil)
	}
}

func (s *subscription) ackMessage(
	ctx context.Context,
	message *firestore.DocumentSnapshot,
	logger watermill.LoggerAdapter,
) error {
	deleteCtx, deleteCancel := context.WithTimeout(ctx, s.config.Timeout)
	defer deleteCancel()
	_, err := message.Ref.Delete(deleteCtx, firestore.Exists)
	if status.Code(err) == codes.NotFound {
		logger.Trace("Message deleted meanwhile", nil)
		return nil
	}
	if err != nil {
		return err
	}

	return nil
}
