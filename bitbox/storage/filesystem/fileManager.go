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
