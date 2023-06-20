package storage

import (
	"fmt"
	"runtime"
	"sync"
)

const SYSTEM_DB_NAME = "_system"
const NODE_STATS_DB_NAME = "_node"
const SYNC_DELETES_COLLECTION_NAME = "_sync_deletes"

type StorageManager struct {
	collections map[string]*Collection
	sync        sync.Mutex
}

var confGoOne sync.Once

func InitStorageManager() *StorageManager {
	//needed by badgedb to improve performance
	confGoOne.Do(func() {
		fmt.Println("GOMAXPROCS set to 128")
		runtime.GOMAXPROCS(128)
	})

	dbManager := StorageManager{
		collections: make(map[string]*Collection),
	}
	return &dbManager
}

func (db *StorageManager) GetCollection(collectionName string) (*Collection, error) {
	if collectionName == SYSTEM_DB_NAME || collectionName == NODE_STATS_DB_NAME || collectionName == SYNC_DELETES_COLLECTION_NAME {
		return nil, ErrCollectionNameReserved
	}

	db.sync.Lock()
	res := db.collections[collectionName]

	if res == nil {
		cn, err := OpenCollection(collectionName)
		if err != nil {
			fmt.Println(err)
			return nil, err
		}

		db.collections[collectionName] = cn
		res = db.collections[collectionName]
	}
	db.sync.Unlock()

	if res == nil {
		return nil, ErrCollectionNotFound
	}
	return res, nil
}

func (db *StorageManager) DeleteCollection(collectionName string) (err error) {
	if collectionName == SYSTEM_DB_NAME || collectionName == NODE_STATS_DB_NAME || collectionName == SYNC_DELETES_COLLECTION_NAME {
		return ErrCollectionNameReserved
	}

	res := db.collections[collectionName]
	if res != nil {
		err = res.DeleteCollection()

		delete(db.collections, collectionName)
	}

	return err
}

func (db *StorageManager) Shutdown() {
	db.sync.Lock()
	for _, collection := range db.collections {
		collection.Close()
	}
	db.sync.Unlock()
}
