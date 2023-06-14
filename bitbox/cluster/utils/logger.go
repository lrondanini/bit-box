package utils

import (
	"log"
	"strings"
)

type Logger interface {
	Trace(string)
	Debug(string)
	Info(string)
	Warn(string)
	Error(error, string)
	Fatal(error, string)
	Panic(error, string)
}

type InternalLogger struct {
	logger Logger
}

var loggerInstance InternalLogger

func InitLogger(l Logger) {
	loggerInstance.logger = l
}

func GetLogger() *InternalLogger {
	return &loggerInstance
}

func (c *InternalLogger) Trace(msg string) {
	if c.logger == nil {
		return
	}
	c.logger.Trace(msg)
}
func (c *InternalLogger) Debug(msg string) {
	if c.logger == nil {
		return
	}
	c.logger.Debug(msg)
}
func (c *InternalLogger) Info(msg string) {
	log.Println(msg)
	if c.logger == nil {
		return
	}
	c.logger.Info(msg)
}
func (c *InternalLogger) Warn(msg string) {
	if c.logger == nil {
		return
	}
	c.logger.Warn(msg)
}
func (c *InternalLogger) Error(err error, msg string) {
	log.Println(msg)
	if c.logger == nil {
		return
	}
	c.logger.Error(err, msg)
}
func (c *InternalLogger) Fatal(err error, msg string) {
	if c.logger == nil {
		return
	}
	c.logger.Fatal(err, msg)
}
func (c *InternalLogger) Panic(err error, msg string) {
	if c.logger == nil {
		return
	}
	c.logger.Panic(err, msg)
}

/*
* Used to intercept and log logs from serf
 */
type SerfLogWriter struct {
	Skip bool
}

func (mw *SerfLogWriter) Write(line []byte) (n int, err error) {
	if mw.Skip {
		return len(line), nil
	}

	str := string(line)

	//fmt.Println(str)

	tmp := strings.Split(str, ":")

	levelString := str
	if len(tmp) > 0 {
		str = ""
		goMsg := false
		for i := 0; i < len(tmp); i++ {
			if strings.Contains(tmp[i], "agent") || strings.Contains(tmp[i], "serf") || strings.Contains(tmp[i], "memberlist") || strings.Contains(tmp[i], "event") || strings.Contains(tmp[i], "query") {
				goMsg = true
				levelString = tmp[i]
			} else if goMsg {
				if str == "" {
					str += strings.Trim(tmp[i], "\n")
				} else {
					str += ": " + strings.Trim(tmp[i], "\n")
				}

			}
		}
	}

	if strings.Contains(levelString, "INFO") {
		loggerInstance.Info("[SERF]" + str)
	} else if strings.Contains(levelString, "WARN") {
		loggerInstance.Warn("[SERF]" + str)
	} else if strings.Contains(levelString, "ERR") {
		loggerInstance.Error(nil, "[SERF]"+str)
	} else if strings.Contains(levelString, "DEBUG") {
		loggerInstance.Debug("[SERF]" + str)
	} else {
		loggerInstance.Error(nil, "[SERF]"+str)
	}
	/*
		if strings.Contains(levelString, "INFO") {
			loggerInstance.Info().Str("LIB", "SERF").Msg(str)
		} else if strings.Contains(levelString, "WARN") {
			loggerInstance.Warn().Str("LIB", "SERF").Msg(str)
		} else if strings.Contains(levelString, "ERR") {
			loggerInstance.Error().Str("LIB", "SERF").Msg(str)
		} else if strings.Contains(levelString, "DEBUG") {
			loggerInstance.Debug().Str("LIB", "SERF").Msg(str)
		} else {
			loggerInstance.Error().Str("LIB", "SERF").Msg(str)
		}
	*/

	return len(line), nil
}

// --- LOGGER FOR TESTING
type LoggerForTesting struct {
}

func (l *LoggerForTesting) Trace(msg string) {
	log.Println("TRACE: ", msg)
}
func (l *LoggerForTesting) Debug(msg string) {
	log.Println("DEBUG: ", msg)
}
func (l *LoggerForTesting) Info(msg string) {
	log.Println("INFO: ", msg)
}
func (l *LoggerForTesting) Warn(msg string) {
	log.Println("WARN: ", msg)
}
func (l *LoggerForTesting) Error(err error, msg string) {
	log.Println("ERROR: ", msg, err)
}
func (l *LoggerForTesting) Fatal(err error, msg string) {
	log.Println("FATAL: ", msg, err)
}
func (l *LoggerForTesting) Panic(err error, msg string) {
	log.Println("PANIC: ", msg, err)
}
