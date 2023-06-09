package storage

import (
	"fmt"
	"runtime"
	"sync"
)

const SYSTEM_COLLECTION_NAME = "_system"

type StorageManager struct {
	collections map[string]*Collection
}

var confGoOne sync.Once

func InitStorageManager() StorageManager {
	//needed by badgedb to improve performance
	confGoOne.Do(func() {
		fmt.Println("ser GOMAXPROCS")
		runtime.GOMAXPROCS(128)
	})

	dbManager := StorageManager{
		collections: make(map[string]*Collection),
	}
	return dbManager
}

func (db *StorageManager) GetCollection(collectionName string) (*Collection, error) {
	if collectionName == SYSTEM_COLLECTION_NAME {
		return nil, ErrCollectionNameReserved
	}

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

	if res == nil {
		return nil, ErrCollectionNotFound
	}
	return res, nil
}

func (db *StorageManager) DeleteCollection(collectionName string) (err error) {
	if collectionName == SYSTEM_COLLECTION_NAME {
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
	for _, collection := range db.collections {
		collection.Close()
	}
}
