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

package utils

import (
	"fmt"
	"time"

	"github.com/mackerelio/go-osstat/cpu"
	"github.com/mackerelio/go-osstat/memory"
)

type HardwareStats struct {
	Memory string
	CPU    string
}

func GetHardwareStats() *HardwareStats {
	res := HardwareStats{
		Memory: "N.A.",
		CPU:    "N.A.",
	}

	//Memory
	memory, err := memory.Get()
	if err == nil {
		res.Memory = byteCountPerHumans(memory.Used) + "/" + byteCountPerHumans(memory.Free) + "/" + byteCountPerHumans(memory.Total)
		// fmt.Println("memory total: ", byteCountPerHumans(memory.Total))
		// fmt.Println("memory used: ", byteCountPerHumans(memory.Used))
		// fmt.Println("memory cached:  ", byteCountPerHumans(memory.Cached))
		// fmt.Println("memory free:  ", byteCountPerHumans(memory.Free))
	}

	//CPU
	before, err := cpu.Get()
	if err == nil {
		time.Sleep(100 * time.Millisecond)
		after, err := cpu.Get()
		if err == nil {
			total := float64(after.Total - before.Total)

			res.CPU = fmt.Sprintf("%.2f", float64(after.User-before.User)/total*100)
		}

	}

	return &res
}

func byteCountPerHumans(b uint64) string {
	const unit = 1000
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB",
		float64(b)/float64(div), "kMGTPE"[exp])
}
