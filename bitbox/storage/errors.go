package storage

import (
	"errors"

	"github.com/dgraph-io/badger/v4"
)

var (
	ErrKeyNotFound            = badger.ErrKeyNotFound
	ErrCollectionNameReserved = errors.New("this collection name cannot be used")
	ErrCollectionNotFound     = errors.New("collection not found")
)
