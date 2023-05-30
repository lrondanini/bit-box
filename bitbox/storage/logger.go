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
