package filesystem

import (
	"bytes"
	"encoding/gob"
	"os"
)

func SimpleBinaryEncode(data interface{}) ([]byte, error) {
	// Encode the entry for storing.
	var buf []byte
	b := bytes.NewBuffer(buf)
	enc := gob.NewEncoder(b)
	err := enc.Encode(data)

	if err != nil {
		return nil, err
	}

	return b.Bytes(), nil
}

func SimpleBinaryDecode(content []byte, writeTo interface{}) error {
	r := bytes.NewReader(content)
	dec := gob.NewDecoder(r)
	return dec.Decode(writeTo)
}

func WriteBinary(filePath string, content interface{}) error {
	encoded, errEncoding := SimpleBinaryEncode(content)
	if errEncoding != nil {
		return errEncoding
	}
	err := os.WriteFile(filePath, encoded, 0644)
	return err
}

/*
readTo need to be passed as pointer, foe example:

	var vnodes []partitioner.VNode
	err := filesystem.ReadBinary("vnodes", &vnodes)
*/
func ReadBinary(filePath string, readTo interface{}) error {

	content, errOpeningFile := os.ReadFile(filePath)
	if errOpeningFile != nil {
		return errOpeningFile
	}

	return SimpleBinaryDecode(content, readTo)
}
