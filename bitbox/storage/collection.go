// Copyright 2023 lucarondanini
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/lrondanini/bit-box/bitbox/cluster/stream"
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

type DbValue struct {
	Hash      uint64
	Timestamp int64
	Value     []byte
}

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

func (c *Collection) DeleteCollection() error {
	c.db.Close()
	return c.db.DropAll()
}

func (c *Collection) BackUp() {
	fmt.Println("TODO: implement backup")

	// fi, err := os.OpenFile("backup.txt", os.O_WRONLY|os.O_CREATE, 0666)
	// if err != nil {
	// 	panic(err)
	// }
	// // close fi on exit and check for its returned error
	// defer func() {
	// 	if err := fi.Close(); err != nil {
	// 		panic(err)
	// 	}
	// }()

	// var backpTill uint64 //increment by 1 (+1) and pass it again into db.Backup(w, backpTill) to get the next backup
	// backpTill, err = db.Backup(fi, 0)
	// if err != nil {
	// 	panic(err)
	// }
	// fmt.Println("Backup done till", backpTill)
}

func (c *Collection) LoadBackUp() {
	fmt.Println("TODO: implement load backup")

	// fi, err := os.OpenFile("backup.txt", os.O_RDONLY, 0666)
	// if err != nil {
	// 	panic(err)
	// }
	// // close fi on exit and check for its returned error
	// defer func() {
	// 	if err := fi.Close(); err != nil {
	// 		panic(err)
	// 	}
	// }()

	// db.Load(fi, 16)
}

func (c *Collection) UpsertFromStreaming(data []stream.StreamEntry, skip func(string, []byte) bool, updateStats func()) {
	txn := c.db.NewTransaction(true)

	for _, entry := range data {
		// node can accept writes while streaming/synching with the cluster,
		// this means that if there is a value its newer than the one coming from the cluster
		isNew := false
		_, err := txn.Get(entry.Key)
		if err != nil {
			if err == ErrKeyNotFound {
				isNew = true
			}
		}

		toDelete := skip(c.name, entry.Key)

		if isNew && !toDelete {
			err := txn.Set(entry.Key, entry.Value)
			if err == badger.ErrTxnTooBig {
				_ = txn.Commit()
				txn = c.db.NewTransaction(true)
				_ = txn.Set(entry.Key, entry.Value)
			}
			updateStats()
		} else if toDelete {
			//present but not deleted
			txn.Delete(entry.Key)
		}

	}

	_ = txn.Commit()

}

func (c *Collection) DeleteKeys(keys [][]byte, updateStats func()) error {
	txn := c.db.NewTransaction(true)

	for _, k := range keys {
		txn.Delete(k)
		updateStats()

	}
	_ = txn.Commit()

	return nil
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

	return c.SetRaw(kBytes, vBytes)
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

	vBytes, err := c.GetRaw(kBytes)

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

func (c *Collection) Exists(key []byte) (bool, error) {
	txn := c.db.NewTransaction(false)
	defer txn.Discard()

	// Use the transaction...
	_, err := txn.Get(key)
	if err != nil {
		if err == ErrKeyNotFound {
			return false, nil
		}
		return false, err
	}

	return true, err
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

func (c *Collection) GetIteratorFromRaw(from []byte) (*Iterator, error) {
	return GetIteratorFromRaw(c.db, from)
}

func (c *Collection) Delete(k interface{}) error {

	kBytes, err := ToBytes(k)

	if err != nil {
		return err
	}

	return c.DeleteRaw(kBytes)
}

func (c *Collection) SetRaw(k []byte, v []byte) error {
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

func (c *Collection) GetRaw(k []byte) ([]byte, error) {
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

func (c *Collection) DeleteRaw(k []byte) error {
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
