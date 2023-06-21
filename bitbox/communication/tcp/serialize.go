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
