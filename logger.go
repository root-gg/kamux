package kamux

// A Logger is the interface used in this package for logging,
// so that any backend can be plugged in.
type Logger interface {
	Printf(format string, args ...interface{})
	Println(args ...interface{})

	Fatalf(format string, args ...interface{})
	Fatal(args ...interface{})

	Panicf(format string, args ...interface{})
	Panic(args ...interface{})
}
