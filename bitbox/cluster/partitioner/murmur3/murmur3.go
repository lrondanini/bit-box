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

package murmur3

func GetHash128(s string) (uint64, uint64) {
	h := New128WithSeed(1000)
	h.Write([]byte(s))
	return h.Sum128()
}

func GetHash64ForString(s string) uint64 {
	h := New64WithSeed(1000)
	h.Write([]byte(s))
	return h.Sum64()
}

func GetHash64(s []byte) uint64 {
	h := New64WithSeed(1000)
	h.Write(s)
	return h.Sum64()
}
