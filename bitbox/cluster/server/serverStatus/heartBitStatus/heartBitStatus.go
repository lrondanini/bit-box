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

package heartBitStatus

import "fmt"

type HeartBitStatus int

const (
	None HeartBitStatus = iota
	Alive
	Leaving
	Left
	Failed
)

func (s HeartBitStatus) String() string {
	switch s {
	case None:
		return "none"
	case Alive:
		return "alive"
	case Leaving:
		return "leaving"
	case Left:
		return "left"
	case Failed:
		return "failed"
	default:
		panic(fmt.Sprintf("unknown MemberStatus: %d", s))
	}
}
