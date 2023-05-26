package storage

import (
	"errors"

	"github.com/lrondanini/bit-box/bitbox/cluster/utils"
	"github.com/syndtr/goleveldb/leveldb"
)

type Collection struct {
	name   string
	folder string
	db     *leveldb.DB
}

func OpenCollection(collectionName string) (*Collection, error) {

	conf := utils.GetClusterConfiguration()

	folder := "./"
	if conf.DATA_FOLDER != "" {
		folder = conf.DATA_FOLDER
		lastChar := folder[len(folder)-1:]
		if lastChar != "/" {
			folder = folder + "/" + collectionName
		} else {
			folder = folder + collectionName
		}
	}

	collection, err := leveldb.OpenFile(folder, nil)
	if err != nil {
		return nil, errors.New("[db-collection-1]" + err.Error())
	}

	return &Collection{
		name:   collectionName,
		folder: folder,
		db:     collection,
	}, nil
}

func (c *Collection) Close() {
	c.db.Close()
}

func (c *Collection) Set(k, v string) error {
	return c.db.Put([]byte(k), []byte(v), nil)
}

func (c *Collection) Get(k string) (string, error) {
	v, err := c.db.Get([]byte(k), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return "", leveldb.ErrNotFound
		}
		return "", errors.New("[db-collection-2]" + err.Error())
	}
	return string(v), nil
}

func (c *Collection) Delete(k string) error {
	return c.db.Delete([]byte(k), nil)
}

func (c *Collection) BatchSet(s map[string]string) error {
	batch := new(leveldb.Batch)
	for k, v := range s {
		batch.Put([]byte(k), []byte(v))
	}
	return c.db.Write(batch, nil)
}

func (c *Collection) BatchDelete(s []string) error {
	batch := new(leveldb.Batch)
	for _, v := range s {
		batch.Delete([]byte(v))
	}
	return c.db.Write(batch, nil)
}
