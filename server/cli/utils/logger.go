package utils

import (
	"io"
	"os"
	"path"
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

func Configure(conf *ClusterConfiguration) *Logger {
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

	/*
		to manage caller's string length in logs (not used here, logs are not coming from cluster anymore)
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
	*/
	logger := zerolog.New(mw).With().Timestamp().Logger()
	return &Logger{
		Logger: &logger,
	}
}

func newRollingFile(conf *ClusterConfiguration) io.Writer {
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

func (c *Logger) Trace(msg string) {
	if c.Logger == nil {
		return
	}
	c.Logger.Trace().Msg(msg)
}
func (c *Logger) Debug(msg string) {
	if c.Logger == nil {
		return
	}
	c.Logger.Debug().Msg(msg)
}
func (c *Logger) Info(msg string) {
	if c.Logger == nil {
		return
	}
	c.Logger.Info().Msg(msg)
}
func (c *Logger) Warn(msg string) {
	if c.Logger == nil {
		return
	}
	c.Logger.Warn().Msg(msg)
}
func (c *Logger) Error(err error, msg string) {
	if c.Logger == nil {
		return
	}
	if err != nil {
		c.Logger.Error().Msg(msg)
	} else {
		c.Logger.Error().Err(err).Msg(msg)
	}

}
func (c *Logger) Fatal(err error, msg string) {
	if c.Logger == nil {
		return
	}
	if err != nil {
		c.Logger.Fatal().Msg(msg)
	} else {
		c.Logger.Fatal().Err(err).Msg(msg)
	}
}
func (c *Logger) Panic(err error, msg string) {
	if c.Logger == nil {
		return
	}
	if err != nil {
		c.Logger.Panic().Msg(msg)
	} else {
		c.Logger.Panic().Err(err).Msg(msg)
	}
}
