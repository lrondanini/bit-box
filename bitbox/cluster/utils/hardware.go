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
