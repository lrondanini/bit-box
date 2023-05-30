package storage

import (
	"errors"
	"path/filepath"
	"reflect"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/lrondanini/bit-box/bitbox/cluster/utils"
)

func DbGarbageCollector(db *badger.DB) {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		lsm, vlog := db.Size()
		if lsm > 1024*1024*8 || vlog > 1024*1024*32 {
			db.RunValueLogGC(0.5)
		}
	}
}

var ErrKeyNotFound = badger.ErrKeyNotFound

type Collection struct {
	name       string
	folder     string
	db         *badger.DB
	logEnabled bool
}

func OpenCollection(collectionName string) (*Collection, error) {
	conf := utils.GetClusterConfiguration()

	folder := "./"
	if conf.DATA_FOLDER != "" {
		folder = conf.DATA_FOLDER
	}
	folder = filepath.Join(folder, collectionName)

	logManager := &dbLogger{
		logger:   utils.GetLogger(),
		disabled: !conf.LOG_STORAGE,
	}

	options := badger.DefaultOptions(folder)
	options.IndexCacheSize = 100 << 20
	options.Logger = logManager

	db, err := badger.Open(options)

	if err != nil {
		return nil, err
	}

	go DbGarbageCollector(db)

	return &Collection{
		name:       collectionName,
		folder:     folder,
		db:         db,
		logEnabled: conf.LOG_STORAGE,
	}, nil
}

func (c *Collection) Close() {
	c.db.Close()
}

func (c *Collection) Set(k interface{}, v interface{}) error {

	kBytes, err := ToBytes(k)

	if err != nil {
		return err
	}

	vBytes, err := EncodeValue(v)

	if err != nil {
		return err
	}

	return c._set(kBytes, vBytes)
}

func (c *Collection) Get(k interface{}, v interface{}) error {

	//verify that v is passed as pointer address (&)
	value := reflect.ValueOf(v)
	if value.Type().Kind() != reflect.Pointer {
		return errors.New("attempt to decode into a non-pointer")
	}

	kBytes, err := ToBytes(k)

	if err != nil {
		return err
	}

	vBytes, err := c._get(kBytes)

	if err != nil {
		return err
	}

	//no need for & on second parameter, it is already a pointer
	err = DecodeValue(vBytes, v)

	if err != nil {
		return err
	}

	return nil
}

func (c *Collection) GetIterator() (*Iterator, error) {
	return GetIterator(c.db)
}

func (c *Collection) GetIteratorFrom(from interface{}) (*Iterator, error) {
	return GetIteratorFrom(c.db, from)
}

func (c *Collection) GetFilteredIterator(from interface{}, till interface{}) (*Iterator, error) {
	return GetFilteredIterator(c.db, from, till)
}

func (c *Collection) Delete(k interface{}) error {

	kBytes, err := ToBytes(k)

	if err != nil {
		return err
	}

	return c._delete(kBytes)
}

func (c *Collection) _set(k []byte, v []byte) error {
	if len(k) == 0 {
		return errors.New("key cannot be empty")
	}
	//empty values can cause problems:
	if len(v) == 0 {
		v = []byte(" ")
	}

	// Start a writable transaction.
	txn := c.db.NewTransaction(true)

	err := txn.Set(k, v)
	if err != nil {
		txn.Discard()
		return err
	}

	// Commit the transaction and check for error (also closes the transaction).
	if err := txn.Commit(); err != nil {
		txn.Discard()
		return err
	}

	return nil
}

func (c *Collection) _get(k []byte) ([]byte, error) {
	txn := c.db.NewTransaction(false)
	defer txn.Discard()

	// Use the transaction...
	item, err := txn.Get(k)
	if err != nil {
		return nil, err
	}

	var valCopy []byte

	valCopy, err = item.ValueCopy(nil)
	if err != nil {
		return nil, err
	}

	return valCopy, err
}

func (c *Collection) _delete(k []byte) error {
	txn := c.db.NewTransaction(true)

	err := txn.Delete(k)
	if err != nil {
		txn.Discard()
		return err
	}

	// Commit the transaction and check for error (also closes the transaction).
	if err := txn.Commit(); err != nil {
		txn.Discard()
		return err
	}

	return nil
}
