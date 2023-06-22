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

package storage

import (
	"fmt"
	"testing"
	"time"
)

func TestConversion(tt *testing.T) {

	var b []byte
	var e error

	var u uint = 5345345345534511678
	fmt.Print("Uint: ", u, " -> ")
	b, e = ToBytes(u)
	if e != nil {
		fmt.Println(e)
	} else {
		var uu uint
		e = FromBytes(b, &uu)
		if e != nil {
			fmt.Println(e)
		} else {
			fmt.Println(uu)
		}

		if uu != u {
			tt.Errorf("Uint conversion failed %v != %v", uu, u)
		}
	}

	var u16 uint16 = 23452
	fmt.Print("Uint16: ", u16, " -> ")
	b, e = ToBytes(u16)
	if e != nil {
		fmt.Println(e)
	} else {
		var uu uint16
		e = FromBytes(b, &uu)
		if e != nil {
			fmt.Println(e)
		} else {
			fmt.Println(uu)
		}
		if uu != u16 {
			tt.Errorf("Uint16 conversion failed %v != %v", uu, u16)
		}
	}

	var u32 uint32 = 63512342
	fmt.Print("Uint32: ", u32, " -> ")
	b, e = ToBytes(u32)
	if e != nil {
		fmt.Println(e)
	} else {
		var uu uint32
		e = FromBytes(b, &uu)
		if e != nil {
			fmt.Println(e)
		} else {
			fmt.Println(uu)
		}
		if uu != u32 {
			tt.Errorf("Uint32 conversion failed %v != %v", uu, u32)
		}
	}

	var u64 uint64 = 1234512341234123412
	fmt.Print("Uint64: ", u64, " -> ")
	b, e = ToBytes(u64)
	if e != nil {
		fmt.Println(e)
	} else {
		var uu uint64
		e = FromBytes(b, &uu)
		if e != nil {
			fmt.Println(e)
		} else {
			fmt.Println(uu)
		}

		if uu != u64 {
			tt.Errorf("Uint64 conversion failed %v != %v", uu, u64)
		}
	}

	var i int = -123412412424241241
	fmt.Print("Int: ", i, " -> ")
	b, e = ToBytes(i)
	if e != nil {
		fmt.Println(e)
	} else {
		var uu int
		e = FromBytes(b, &uu)
		if e != nil {
			fmt.Println(e)
		} else {
			fmt.Println(uu)
		}

		if uu != i {
			tt.Errorf("Int conversion failed %v != %v", uu, i)
		}
	}

	var i16 int16 = -4234
	fmt.Print("Int16: ", i16, " -> ")
	b, e = ToBytes(i16)
	if e != nil {
		fmt.Println(e)
	} else {
		var uu int16
		e = FromBytes(b, &uu)
		if e != nil {
			fmt.Println(e)
		} else {
			fmt.Println(uu)
		}

		if uu != i16 {
			tt.Errorf("Int16 conversion failed %v != %v", uu, i16)
		}
	}

	var i32 int32 = -123452323
	fmt.Print("Int32: ", i32, " -> ")
	b, e = ToBytes(i32)
	if e != nil {
		fmt.Println(e)
	} else {
		var uu int32
		e = FromBytes(b, &uu)
		if e != nil {
			fmt.Println(e)
		} else {
			fmt.Println(uu)
		}

		if uu != i32 {
			tt.Errorf("Int32 conversion failed %v != %v", uu, i32)
		}
	}

	var i64 int64 = -1234
	fmt.Print("Int64: ", i64, " -> ")
	b, e = ToBytes(i64)
	if e != nil {
		fmt.Println(e)
	} else {
		var uu int64
		e = FromBytes(b, &uu)
		if e != nil {
			fmt.Println(e)
		} else {
			fmt.Println(uu)
		}

		if uu != i64 {
			tt.Errorf("Int64 conversion failed %v != %v", uu, i64)
		}
	}

	var f64 float64 = -234.023
	fmt.Print("Float64: ", fmt.Sprintf("%f", f64), " -> ")
	b, e = ToBytes(f64)
	if e != nil {
		fmt.Println(e)
	} else {
		var uu float64
		e = FromBytes(b, &uu)
		if e != nil {
			fmt.Println(e)
		} else {
			fmt.Println(uu)
		}

		if uu != f64 {
			tt.Errorf("Float64 conversion failed %v != %v", uu, f64)
		}
	}

	var t = time.Now()
	fmt.Print("Time: ", t, " -> ")
	b, e = ToBytes(t)
	if e != nil {
		fmt.Println(e)
	} else {
		var uu time.Time
		e = FromBytes(b, &uu)
		if e != nil {
			fmt.Println(e)
		} else {
			fmt.Println(uu)
		}

		if uu.UnixNano() != t.UnixNano() {
			tt.Errorf("Time conversion failed %v != %v", uu, t)
		}
	}

	var s = "Hello World!"
	fmt.Print("String: ", s, " -> ")
	b, e = ToBytes(s)
	if e != nil {
		fmt.Println(e)
	} else {
		var uu string
		e = FromBytes(b, &uu)
		if e != nil {
			fmt.Println(e)
		} else {
			fmt.Println(uu)
		}

		if uu != s {
			tt.Errorf("String conversion failed %v != %v", uu, s)
		}
	}
}
