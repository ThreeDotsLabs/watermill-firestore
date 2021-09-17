package firestore

import (
	"context"

	"cloud.google.com/go/firestore"
)

// client defines all the methods original client implements.
// It's meant to be used everywhere client is needed in order to enable decorating the original implementation.
type client interface {
	RunTransaction(
		ctx context.Context,
		f func(context.Context, *firestore.Transaction) error,
		opts ...firestore.TransactionOption,
	) (err error)
	Collection(path string) *firestore.CollectionRef
	Doc(path string) *firestore.DocumentRef
	CollectionGroup(collectionID string) *firestore.CollectionGroupRef
	GetAll(ctx context.Context, docRefs []*firestore.DocumentRef) ([]*firestore.DocumentSnapshot, error)
	Collections(ctx context.Context) *firestore.CollectionIterator
	Batch() *firestore.WriteBatch
	Close() error
}
