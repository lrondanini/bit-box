package storage

import (
	"bytes"
	"errors"
	"reflect"

	"github.com/dgraph-io/badger/v4"
)

/*
Usage:

test, err := storage.OpenCollection("test")

	if err != nil {
		panic(err)
	}
	for i := 0; i < 10; i++ {
		fmt.Println(i)
		test.Set(i, "test")
	}

	it, err := test.GetFilteredIterator(2, 7)
	if err != nil {
		panic(err)
	}
	for it.HasMore() {
		var v string
		var k int
		err := it.Next(&k, &v)
		if err != nil {
			panic(err)
		}
		fmt.Println(k, "=", v)
	}

	it, err := test.GetIterator()
	if err != nil {
		panic(err)
	}
	for it.HasMore() {
		var v string
		var k int
		err := it.Next(&k, &v)
		if err != nil {
			panic(err)
		}
		fmt.Println(k, "=", v)
	}
	it.Close()
*/
type Iterator struct {
	it        *badger.Iterator
	from      interface{}
	till      interface{}
	bytesFrom []byte
	bytesTill []byte
	hasMore   bool
	started   bool
}

func GetIterator(db *badger.DB) (*Iterator, error) {
	txn := db.NewTransaction(false)
	opts := badger.DefaultIteratorOptions
	opts.PrefetchSize = 10
	opts.PrefetchValues = false // key-only iteration. It is several order of magnitudes faster than regular iteration
	//opts.Reverse = reverse

	res := &Iterator{
		it:      txn.NewIterator(opts),
		started: false,
	}

	res.it.Rewind() //to the top

	return res, nil
}

func GetIteratorFrom(db *badger.DB, from interface{}) (*Iterator, error) {
	txn := db.NewTransaction(false)
	opts := badger.DefaultIteratorOptions
	opts.PrefetchSize = 10
	opts.PrefetchValues = false // key-only iteration. It is several order of magnitudes faster than regular iteration
	//opts.Reverse = reverse

	bFrom, err := ToBytes(from)

	if err != nil {
		return nil, err
	}

	res := &Iterator{
		it:        txn.NewIterator(opts),
		from:      from,
		bytesFrom: bFrom,
		started:   false,
	}

	res.it.Seek(bFrom)

	return res, nil
}

func GetFilteredIterator(db *badger.DB, from interface{}, till interface{}) (*Iterator, error) {
	txn := db.NewTransaction(false)
	opts := badger.DefaultIteratorOptions
	opts.PrefetchSize = 10
	opts.PrefetchValues = false // key-only iteration. It is several order of magnitudes faster than regular iteration
	//opts.Reverse = reverse

	bFrom, err := ToBytes(from)

	if err != nil {
		return nil, err
	}
	bTill, err := ToBytes(till)
	if err != nil {
		return nil, err
	}

	res := &Iterator{
		it:        txn.NewIterator(opts),
		from:      from,
		till:      till,
		bytesFrom: bFrom,
		bytesTill: bTill,
		started:   false,
	}

	res.it.Seek(bFrom)

	return res, nil
}

func GetIteratorFromRaw(db *badger.DB, from []byte) (*Iterator, error) {
	txn := db.NewTransaction(false)
	opts := badger.DefaultIteratorOptions
	opts.PrefetchSize = 10
	opts.PrefetchValues = false // key-only iteration. It is several order of magnitudes faster than regular iteration
	//opts.Reverse = reverse

	res := &Iterator{}

	if len(from) > 0 {
		res = &Iterator{
			it:        txn.NewIterator(opts),
			from:      from,
			bytesFrom: from,
			started:   false,
		}

		res.it.Seek(from)
	} else {
		res = &Iterator{
			it:      txn.NewIterator(opts),
			started: false,
		}

		res.it.Rewind() //to the top
	}

	return res, nil
}

func (i *Iterator) HasMore() bool {
	it := i.it
	if !i.started {
		i.started = true
		i.hasMore = it.Valid()
	} else {
		if i.hasMore {
			it.Next()
			i.hasMore = it.Valid()
		}
	}

	return i.hasMore
}

func (i *Iterator) HasMoreWithPrefix(prefix interface{}) bool {
	pBytes, _ := ToBytes(prefix)
	it := i.it
	if !i.started {
		i.started = true
		i.hasMore = it.ValidForPrefix(pBytes)
	} else {
		if i.hasMore {
			it.Next()
			i.hasMore = it.ValidForPrefix(pBytes)
		}
	}

	return i.hasMore
}

func (i *Iterator) Next(key interface{}, value interface{}) error {
	vv := reflect.ValueOf(value)
	kk := reflect.ValueOf(key)

	if vv.Type().Kind() != reflect.Pointer || kk.Type().Kind() != reflect.Pointer {
		if vv.Type().Kind() != reflect.Pointer && kk.Type().Kind() != reflect.Pointer {
			return errors.New("attempt to decode into a non-pointer (key, value)")
		} else if kk.Type().Kind() != reflect.Pointer {
			return errors.New("attempt to decode into a non-pointer (key)")
		} else {
			return errors.New("attempt to decode into a non-pointer (value)")
		}
	}

	it := i.it

	var kBytes []byte

	if i.hasMore {
		item := it.Item()
		kBytes = item.Key()
		v := DbValue{}
		err := item.Value(func(vBytes []byte) error {
			return DecodeValue(vBytes, &v)
		})
		if err != nil {
			return err
		}
		if bytes.Equal(kBytes, i.bytesTill) {
			i.hasMore = false
		}

		err = FromBytes(kBytes, key)
		if err != nil {
			return err
		}
		err = FromBytes(v.Value, value)
		if err != nil {
			return err
		}
	}

	return nil
}

func (i *Iterator) NextRaw() (hash uint64, key []byte, value []byte, err error) {
	it := i.it

	v := DbValue{}

	if i.hasMore {
		item := it.Item()
		key = item.Key()
		err := item.Value(func(vBytes []byte) error {
			value = vBytes
			return DecodeValue(vBytes, &v)
		})
		if err != nil {
			return 0, nil, nil, err
		}

		hash = v.Hash
	}

	return hash, key, value, err
}

func (i *Iterator) Close() {
	i.it.Close()
}
