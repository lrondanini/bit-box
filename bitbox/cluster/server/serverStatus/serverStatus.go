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

package serverStatus

import "fmt"

type ServerStatus uint8

/*
Used to monitor node's status - see heartbit for the statuses used in the cluster's heartbeat
*/
const (
	Joining ServerStatus = iota
	Decommissioning
	Alive
	Starting
)

func (s ServerStatus) String() string {
	switch s {
	case Joining:
		return "Joining"
	case Decommissioning:
		return "Decommissioning"
	case Alive:
		return "Alive"
	case Starting:
		return "Starting"
	default:
		panic(fmt.Sprintf("unknown ServerStatus: %d", s))
	}
}
