package filesystem

import (
	"bytes"
	"encoding/gob"
	"errors"
	"os"

	"github.com/lrondanini/bit-box/bitbox/cluster/utils"
)

func preparePath(fileName string) string {
	conf := utils.GetClusterConfiguration()

	var path = conf.DATA_FOLDER
	var filePath string
	if path == "" {
		filePath = "./" + fileName
	} else {
		lastChar := path[len(path)-1:]
		if lastChar != "/" {
			filePath = path + "/" + fileName
		} else {
			filePath = path + fileName
		}
	}
	return filePath
}

func Append(fileName string) {

}

func encode(data interface{}) ([]byte, error) {
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

func decode(content []byte, writeTo interface{}) error {
	r := bytes.NewReader(content)
	dec := gob.NewDecoder(r)
	return dec.Decode(writeTo)
}

func WriteBinary(fileName string, content interface{}) error {
	if fileName == "" {
		return errors.New("empty fileName")
	}

	filePath := preparePath(fileName)

	encoded, errEncoding := encode(content)
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
func ReadBinary(fileName string, readTo interface{}) error {
	if fileName == "" {
		return errors.New("empty fileName")
	}

	filePath := preparePath(fileName)

	content, errOpeningFile := os.ReadFile(filePath)
	if errOpeningFile != nil {
		return errOpeningFile
	}

	return decode(content, readTo)
}
