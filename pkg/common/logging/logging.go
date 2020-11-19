package logging

// Logger represents an interface around https://github.com/kentik/golog
type Logger interface {
	Debugf(prefix, format string, v ...interface{})
	Infof(prefix, format string, v ...interface{})
	Errorf(prefix, format string, v ...interface{})
	Warnf(prefix, format string, v ...interface{})
}
