package tcp

import (
	"bytes"
	"encoding/base64"
	"encoding/gob"
)

func EncodeBody(data interface{}) (string, error) {
	var buf []byte
	b := bytes.NewBuffer(buf)
	enc := gob.NewEncoder(b)
	err := enc.Encode(data)

	if err != nil {
		return "", err
	}

	return base64.StdEncoding.EncodeToString(b.Bytes()), nil
}

func DecodeBody(content string, writeTo interface{}) error {

	binary, err := base64.StdEncoding.DecodeString(content)

	if err != nil {
		return err
	}

	r := bytes.NewReader(binary)
	dec := gob.NewDecoder(r)
	return dec.Decode(writeTo)
}
