package firestore

import (
	"cloud.google.com/go/firestore"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
)

// Marshaler marshals and unmarshals Watermill messages for storage in
// Firestore.
type Marshaler interface {
	// Marshal should return a data type which is supported by Firestore.
	// See the docstring on cloud.google.com/go/firestore.DocumentRef.Create.
	Marshal(msg *message.Message) (interface{}, error)

	// Unmarshal should set the following message fields: UUID, Metadata,
	// Payload.
	Unmarshal(doc *firestore.DocumentSnapshot) (*message.Message, error)
}

type Message struct {
	UUID     string            `firestore:"uuid"`
	Metadata map[string]string `firestore:"metadata"`
	Payload  string            `firestore:"payload"`
}

type DefaultMarshaler struct{}

func (d DefaultMarshaler) Marshal(msg *message.Message) (interface{}, error) {
	firestoreMsg := Message{
		UUID:     msg.UUID,
		Payload:  string(msg.Payload),
		Metadata: make(map[string]string),
	}
	for k, v := range msg.Metadata {
		firestoreMsg.Metadata[k] = v
	}

	return firestoreMsg, nil
}

func (d DefaultMarshaler) Unmarshal(doc *firestore.DocumentSnapshot) (*message.Message, error) {
	fsMsg := Message{}
	if err := doc.DataTo(&fsMsg); err != nil {
		return nil, errors.Wrap(err, "couldn't unmarshal message")
	}

	msg := message.NewMessage(fsMsg.UUID, []byte(fsMsg.Payload))
	for k, v := range fsMsg.Metadata {
		msg.Metadata.Set(k, v)
	}

	return msg, nil
}
