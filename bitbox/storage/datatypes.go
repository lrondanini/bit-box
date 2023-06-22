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
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"math"
	"reflect"
	"strconv"
	"time"
)

/*
uint8 (0 to 255)
uint16 (0 to 65535)
uint32 (0 to 4294967295)
uint64 (0 to 18446744073709551615)
int8 (-128 to 127)
int16 (-32768 to 32767)
int32 (-2147483648 to 2147483647)
int64 (-9223372036854775808 to 9223372036854775807)

float32
float64

uint (32 or 64 bits)
int (32 or 64 bits)

unix timestamp = int64 (64 bits)
*/

// Encodes to bynary preserving order
func ToBytes(i interface{}) (k []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			switch x := r.(type) {
			case string:
				err = errors.New(x)
			case error:
				err = x
			default:
				err = errors.New("unknown panic")
			}
			k = nil
		}
	}()

	switch i.(type) {
	case uint:
		if strconv.IntSize == 32 {
			k = make([]byte, 4)
			binary.BigEndian.PutUint32(k, uint32(reflect.ValueOf(i).Interface().(uint)))
		} else if strconv.IntSize == 64 {
			k = make([]byte, 8)
			binary.BigEndian.PutUint64(k, uint64(reflect.ValueOf(i).Interface().(uint)))
		}
	case uint16:
		k = make([]byte, 2)
		binary.BigEndian.PutUint16(k, uint16(reflect.ValueOf(i).Interface().(uint16)))
	case uint32:
		k = make([]byte, 4)
		binary.BigEndian.PutUint32(k, uint32(reflect.ValueOf(i).Interface().(uint32)))
	case uint64:
		k = make([]byte, 8)
		binary.BigEndian.PutUint64(k, uint64(reflect.ValueOf(i).Interface().(uint64)))
	case int:
		if strconv.IntSize == 32 {
			k = make([]byte, 4)
			var max int = math.MaxInt32 //2147483647
			o := reflect.ValueOf(i).Interface().(int) + max
			binary.BigEndian.PutUint32(k, uint32(o))
		} else if strconv.IntSize == 64 {
			k = make([]byte, 8)
			var max int = math.MaxInt64 //9223372036854775807
			o := reflect.ValueOf(i).Interface().(int) + max
			binary.BigEndian.PutUint64(k, uint64(o))
		}
	case int16:
		k = make([]byte, 2)
		var max int16 = math.MaxInt16 //32767
		o := reflect.ValueOf(i).Interface().(int16) + max
		binary.BigEndian.PutUint16(k, uint16(o))
	case int32:
		k = make([]byte, 4)
		var max int32 = math.MaxInt32 //2147483647
		o := reflect.ValueOf(i).Interface().(int32) + max
		binary.BigEndian.PutUint32(k, uint32(o))
	case int64:
		k = make([]byte, 8)
		var max int64 = math.MaxInt64 //9223372036854775807
		o := reflect.ValueOf(i).Interface().(int64) + max
		binary.BigEndian.PutUint64(k, uint64(o))
	case float64:
		k = make([]byte, 8)
		binary.BigEndian.PutUint64(k[:], math.Float64bits(reflect.ValueOf(i).Interface().(float64)))
	case time.Time:
		k = make([]byte, 8)
		t := reflect.ValueOf(i).Interface().(time.Time).UnixNano()
		var max int64 = math.MaxInt64 //9223372036854775807
		o := t + max
		binary.BigEndian.PutUint64(k, uint64(o))
	case string:
		k = []byte(reflect.ValueOf(i).String())
	}

	return k, err
}

func FromBytes(i []byte, t interface{}) (err error) {
	value := reflect.ValueOf(t)
	// If e represents a value as opposed to a pointer, the answer won't
	// get back to the caller. Make sure it's a pointer.
	if value.Type().Kind() != reflect.Pointer {
		return errors.New("attempt to decode into a non-pointer")
	}

	defer func() {
		if r := recover(); r != nil {
			switch x := r.(type) {
			case string:
				err = errors.New(x)
			case error:
				err = x
			default:
				err = errors.New("unknown panic")
			}
		}
	}()

	switch value.Elem().Interface().(type) {
	case uint:
		if strconv.IntSize == 32 {
			x := binary.BigEndian.Uint32(i)
			value.Elem().SetUint(uint64(x))
		} else if strconv.IntSize == 64 {
			x := binary.BigEndian.Uint64(i)
			value.Elem().SetUint(x)
		}
	case uint16:
		x := binary.BigEndian.Uint16(i)
		value.Elem().SetUint(uint64(x))
	case uint32:
		x := binary.BigEndian.Uint32(i)
		value.Elem().SetUint(uint64(x))
	case uint64:
		x := binary.BigEndian.Uint64(i)
		value.Elem().SetUint(uint64(x))
	case int:
		if strconv.IntSize == 32 {
			var max uint32 = math.MaxInt32 //2147483647
			tmp := binary.BigEndian.Uint32(i)
			x := int32(tmp - max)
			value.Elem().SetInt(int64(x))
		} else if strconv.IntSize == 64 {
			var max uint64 = math.MaxInt64 //9223372036854775807
			tmp := binary.BigEndian.Uint64(i)
			x := int64(tmp - max)
			value.Elem().SetInt(int64(x))
		}
	case int16:
		var max uint16 = math.MaxInt16 //32767
		tmp := binary.BigEndian.Uint16(i)
		x := int16(tmp - max)
		value.Elem().SetInt(int64(x))
	case int32:
		var max uint32 = math.MaxInt32 // 2147483647
		tmp := binary.BigEndian.Uint32(i)
		x := uint32(tmp - max)
		value.Elem().SetInt(int64(x))
	case int64:
		var max uint64 = math.MaxInt64 //9223372036854775807
		tmp := binary.BigEndian.Uint64(i)
		x := int64(tmp - max)
		value.Elem().SetInt(int64(x))
	case float64:
		x := math.Float64frombits(binary.BigEndian.Uint64(i))
		value.Elem().SetFloat(x)
	case time.Time:
		var max uint64 = math.MaxInt64 //9223372036854775807
		tmp := binary.BigEndian.Uint64(i)
		ct := int64(tmp - max)
		x := time.Unix(0, ct)
		value.Elem().Set(reflect.ValueOf(x))
	case string:
		x := string(i)
		value.Elem().SetString(x)
	}

	return err
}

func EncodeValue(data interface{}) ([]byte, error) {
	var buf []byte
	b := bytes.NewBuffer(buf)
	enc := gob.NewEncoder(b)
	err := enc.Encode(data)

	if err != nil {
		return nil, err
	}

	return b.Bytes(), nil
}

func DecodeValue(content []byte, writeTo interface{}) error {
	r := bytes.NewReader(content)
	dec := gob.NewDecoder(r)
	return dec.Decode(writeTo)
}

func ToString(data interface{}) (string, error) {
	var buf []byte
	b := bytes.NewBuffer(buf)
	enc := gob.NewEncoder(b)
	err := enc.Encode(data)

	if err != nil {
		return "", err
	}

	return base64.StdEncoding.EncodeToString(b.Bytes()), nil
}

func FromString(content string, writeTo interface{}) error {

	binary, err := base64.StdEncoding.DecodeString(content)

	if err != nil {
		return err
	}

	r := bytes.NewReader(binary)
	dec := gob.NewDecoder(r)
	return dec.Decode(writeTo)
}
