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
	"strings"

	"github.com/lrondanini/bit-box/bitbox/cluster/utils"
)

type dbLogger struct {
	disabled bool
	logger   *utils.InternalLogger
}

func (l *dbLogger) Errorf(format string, args ...interface{}) {
	if !l.disabled {
		l.logger.Error(nil, strings.Trim(fmt.Sprintf(format, args...), "\n"))
	}
}
func (l *dbLogger) Warningf(format string, args ...interface{}) {
	if !l.disabled {
		l.logger.Warn(strings.Trim(fmt.Sprintf(format, args...), "\n"))
	}
}

func (l *dbLogger) Infof(format string, args ...interface{}) {
	if !l.disabled {
		l.logger.Info(strings.Trim(fmt.Sprintf(format, args...), "\n"))
	}
}
func (l *dbLogger) Debugf(format string, args ...interface{}) {
	if !l.disabled {
		l.logger.Debug(strings.Trim(fmt.Sprintf(format, args...), "\n"))
	}
}
