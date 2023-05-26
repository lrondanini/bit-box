package utils

import (
	"io"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"

	"github.com/rs/zerolog"
	"gopkg.in/natefinch/lumberjack.v2"
)

var logInstance *Logger
var loggerOnce sync.Once

func GetLogger() *Logger {
	loggerOnce.Do(func() {
		logInstance = Configure(GetClusterConfiguration())
	})
	return logInstance
}

type Logger struct {
	*zerolog.Logger
}

func Configure(conf *Configuration) *Logger {
	var writers []io.Writer

	if conf.LOG_TO == "console" {
		writers = append(writers, zerolog.ConsoleWriter{Out: os.Stderr})
	} else if conf.LOG_TO == "file" {
		writers = append(writers, newRollingFile(conf))
	} else if conf.LOG_TO == "both" {
		writers = append(writers, zerolog.ConsoleWriter{Out: os.Stderr})
		writers = append(writers, newRollingFile(conf))
	}

	mw := io.MultiWriter(writers...)

	switch conf.LOG_LEVEL {
	case "trace":
		zerolog.SetGlobalLevel(zerolog.TraceLevel)
	case "debug":
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	case "info":
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	case "warn":
		zerolog.SetGlobalLevel(zerolog.WarnLevel)
	case "error":
		zerolog.SetGlobalLevel(zerolog.ErrorLevel)
	case "fatal":
		zerolog.SetGlobalLevel(zerolog.FatalLevel)
	case "panic":
		zerolog.SetGlobalLevel(zerolog.PanicLevel)
	}

	zerolog.CallerMarshalFunc = func(pc uintptr, file string, line int) string {
		short := ""
		tmp := strings.Split(file, "/")
		counter := 0
		for i := len(tmp) - 1; i > 0; i-- {
			counter++
			if counter > 3 {
				short = "..../" + tmp[i] + "/" + short
				break
			}
			if short == "" {
				short = tmp[i]
			} else {
				short = tmp[i] + "/" + short
			}

		}
		file = short
		return file + ":" + strconv.Itoa(line)
	}

	logger := zerolog.New(mw).With().Timestamp().Caller().Logger()

	return &Logger{
		Logger: &logger,
	}
}

func newRollingFile(conf *Configuration) io.Writer {
	if conf.LOG_DIR == "" {
		conf.LOG_DIR = "./bit-box-logs"
	}
	if conf.LOG_FILE_NAME == "" {
		conf.LOG_FILE_NAME = "log.txt"
	}

	if conf.LOG_FILE_MAX_SIZE == 0 {
		// MaxSize the max size in MB of the logfile before it's rolled
		conf.LOG_FILE_MAX_SIZE = 20
	}

	if conf.LOG_FILE_MAX_NUM_BACKUPS == 0 {
		// MaxBackups the max number of rolled files to keep
		conf.LOG_FILE_MAX_SIZE = 10
	}

	if conf.LOG_FILE_MAX_AGE == 0 {
		// MaxAge the max age in days to keep a logfile
		conf.LOG_FILE_MAX_SIZE = 10
	}

	return &lumberjack.Logger{
		Filename:   path.Join(conf.LOG_DIR, conf.LOG_FILE_NAME),
		MaxBackups: conf.LOG_FILE_MAX_NUM_BACKUPS, // files
		MaxSize:    conf.LOG_FILE_MAX_SIZE,        // megabytes
		MaxAge:     conf.LOG_FILE_MAX_AGE,         // days
	}
}

/*
* Used to intercept and log logs from serf
 */
type SerfLogWriter struct{}

func (mw *SerfLogWriter) Write(line []byte) (n int, err error) {
	str := string(line)

	tmp := strings.Split(str, ":")

	levelString := str
	if len(tmp) > 0 {
		str = ""
		goMsg := false
		for i := 0; i < len(tmp); i++ {
			if strings.Contains(tmp[i], "agent") || strings.Contains(tmp[i], "serf") {
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
		logInstance.Info().Str("LIB", "SERF").Msg(str)
	} else if strings.Contains(levelString, "WARN") {
		logInstance.Warn().Str("LIB", "SERF").Msg(str)
	} else if strings.Contains(levelString, "ERR") {
		logInstance.Error().Str("LIB", "SERF").Msg(str)
	} else if strings.Contains(levelString, "DEBUG") {
		logInstance.Debug().Str("LIB", "SERF").Msg(str)
	} else {
		logInstance.Error().Str("LIB", "SERF").Msg(str)
	}

	return len(line), nil
}
