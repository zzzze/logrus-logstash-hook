package logrustash

import (
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

// Hook represents a Logstash hook.
// It has two fields: writer to write the entry to Logstash and
// formatter to format the entry to a Logstash format before sending.
//
// To initialize it use the `New` function.
//
type Hook struct {
	writer              io.Writer
	formatter           logrus.Formatter
	healthCheckInterval time.Duration
	retryDelay          time.Duration
	maxRetryCount       int
	connectFailCallback func(error)
	stopCheckAfterfail  bool
	failed              bool
	debug               bool
}

// New returns a new logrus.Hook for Logstash.
//
// To create a new hook that sends logs to `tcp://logstash.corp.io:9999`:
//
// conn, _ := net.Dial("tcp", "logstash.corp.io:9999")
// hook := logrustash.New(conn, logrustash.DefaultFormatter())
func New(w io.Writer, f logrus.Formatter) logrus.Hook {
	return &Hook{
		writer:    w,
		formatter: f,
	}
}

// Options for NewWithHealthCheck
type Options func(*Hook)

// SetHealthCheckInterval set option of check health interval.
func SetHealthCheckInterval(interval time.Duration) Options {
	return func(h *Hook) {
		h.healthCheckInterval = interval
	}
}

// SetRetryDelay set option of reconnect delay.
func SetRetryDelay(interval time.Duration) Options {
	return func(h *Hook) {
		h.retryDelay = interval
	}
}

// SetRetryCount set option of reconnect count.
// if connection is broken, it will try to reconnect with several times.
func SetRetryCount(count int) Options {
	return func(h *Hook) {
		h.maxRetryCount = count
	}
}

// SetFormatter set option of logrus formatter.
func SetFormatter(f logrus.Formatter) Options {
	return func(h *Hook) {
		h.formatter = f
	}
}

// SetConnectFailCallback set option of callback triggered when connection is broken.
// Once connection is broken and reconnect fails, we can send notivication in the callback.
func SetConnectFailCallback(f func(error)) Options {
	return func(h *Hook) {
		h.connectFailCallback = f
	}
}

// SetStopCheckAfterFailed set option of whether stop health check if connection is broken.
func SetStopCheckAfterFailed(s bool) Options {
	return func(h *Hook) {
		h.stopCheckAfterfail = s
	}
}

// SetDebug set option of whether show debug infomation.
func SetDebug(s bool) Options {
	return func(h *Hook) {
		h.debug = s
	}
}

// NewWithHealthCheck returns a new logrus.Hook for Logstash and check connection health in background.
//
// To create a new hook that sends logs to `tcp://logstash.corp.io:9999`:
//
// hook := logrustash.NewWithHealthCheck(
//   "tcp",
//   "logstash.corp.io:9999",
//   logrustash.SetDebug(false),
//   logrustash.SetRetryCount(100),
// }),
func NewWithHealthCheck(network, address string, options ...Options) (logrus.Hook, error) {
	defaultRetryDelay := time.Second * 3
	hook := &Hook{
		formatter:           DefaultFormatter(logrus.Fields{}),
		healthCheckInterval: time.Second * 30,
		retryDelay:          defaultRetryDelay,
		maxRetryCount:       100,
		stopCheckAfterfail:  false,
		failed:              false,
	}
	conn, err := hook.connect(network, address, 0)
	if err != nil {
		return nil, err
	}
	hook.writer = conn
	for _, opt := range options {
		opt(hook)
	}
	go hook.healthCheck(network, address)
	return hook, nil
}

func (h *Hook) healthCheck(network, address string) {
	for {
		if h.failed && h.stopCheckAfterfail {
			break
		}
		if h.debug {
			fmt.Println("[logrus-logstash-hook] health check start")
		}
		time.Sleep(h.healthCheckInterval)
		conn := h.writer.(net.Conn)
		if h.isConnectionClosed(conn) {
			if conn, err := h.connect(network, address, h.maxRetryCount); err == nil {
				h.writer = conn
			} else {
				h.failed = true
				if h.connectFailCallback != nil {
					h.connectFailCallback(err)
				}
				if h.debug {
					fmt.Printf("[logrus-logstash-hook] dial %s %s failed\n", network, address)
				}
			}
		}
	}
}

func (h *Hook) isConnectionClosed(c net.Conn) bool {
	one := make([]byte, 1)
	c.SetReadDeadline(time.Now())
	if _, err := c.Read(one); err != nil {
		if err == io.EOF {
			c.Close()
			c = nil
			return true
		}
		if err, ok := err.(net.Error); ok && err.Timeout() {
			return true
		}
		if h.debug {
			fmt.Printf("[logrus-logstash-hook] read fail: %+v\n", err)
		}
	}
	var zero time.Time
	c.SetReadDeadline(zero)
	return false
}

func (h *Hook) connect(network, address string, retryTimes int) (net.Conn, error) {
	if h.debug {
		fmt.Printf("[logrus-logstash-hook] connect, retryTimes = %d\n", retryTimes)
	}
	conn, err := net.Dial(network, address)
	if err != nil {
		if retryTimes > 0 {
			time.Sleep(h.retryDelay)
			return h.connect(network, address, retryTimes-1)
		}
		return nil, err
	}
	return conn, nil
}

// Fire takes, formats and sends the entry to Logstash.
// Hook's formatter is used to format the entry into Logstash format
// and Hook's writer is used to write the formatted entry to the Logstash instance.
func (h *Hook) Fire(e *logrus.Entry) error {
	dataBytes, err := h.formatter.Format(e)
	if err != nil {
		return err
	}
	_, err = h.writer.Write(dataBytes)
	return err
}

// Levels returns all logrus levels.
func (h *Hook) Levels() []logrus.Level {
	return logrus.AllLevels
}

// Using a pool to re-use of old entries when formatting Logstash messages.
// It is used in the Fire function.
var entryPool = sync.Pool{
	New: func() interface{} {
		return &logrus.Entry{}
	},
}

// copyEntry copies the entry `e` to a new entry and then adds all the fields in `fields` that are missing in the new entry data.
// It uses `entryPool` to re-use allocated entries.
func copyEntry(e *logrus.Entry, fields logrus.Fields) *logrus.Entry {
	ne := entryPool.Get().(*logrus.Entry)
	ne.Message = e.Message
	ne.Level = e.Level
	ne.Time = e.Time
	ne.Data = logrus.Fields{}
	for k, v := range fields {
		ne.Data[k] = v
	}
	for k, v := range e.Data {
		ne.Data[k] = v
	}
	return ne
}

// releaseEntry puts the given entry back to `entryPool`. It must be called if copyEntry is called.
func releaseEntry(e *logrus.Entry) {
	entryPool.Put(e)
}

// LogstashFormatter represents a Logstash format.
// It has logrus.Formatter which formats the entry and logrus.Fields which
// are added to the JSON message if not given in the entry data.
//
// Note: use the `DefaultFormatter` function to set a default Logstash formatter.
type LogstashFormatter struct {
	logrus.Formatter
	logrus.Fields
}

var (
	logstashFields   = logrus.Fields{"@version": "1", "type": "log"}
	logstashFieldMap = logrus.FieldMap{
		logrus.FieldKeyTime: "@timestamp",
		logrus.FieldKeyMsg:  "message",
	}
)

// DefaultFormatter returns a default Logstash formatter:
// A JSON format with "@version" set to "1" (unless set differently in `fields`,
// "type" to "log" (unless set differently in `fields`),
// "@timestamp" to the log time and "message" to the log message.
//
// Note: to set a different configuration use the `LogstashFormatter` structure.
func DefaultFormatter(fields logrus.Fields) logrus.Formatter {
	for k, v := range logstashFields {
		if _, ok := fields[k]; !ok {
			fields[k] = v
		}
	}

	return LogstashFormatter{
		Formatter: &logrus.JSONFormatter{FieldMap: logstashFieldMap},
		Fields:    fields,
	}
}

// Format formats an entry to a Logstash format according to the given Formatter and Fields.
//
// Note: the given entry is copied and not changed during the formatting process.
func (f LogstashFormatter) Format(e *logrus.Entry) ([]byte, error) {
	ne := copyEntry(e, f.Fields)
	dataBytes, err := f.Formatter.Format(ne)
	releaseEntry(ne)
	return dataBytes, err
}
