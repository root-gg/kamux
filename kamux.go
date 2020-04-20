package kamux

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/Shopify/sarama"
)

/*

	Kamux
	A handy class which simplify consuming, and distributing work from a kafka cluster

	This class will :
		- Consume one or multiple topics, with sarama cluster library
		- Shard work per partition (which will keep the order of messages)
		- For each message, exec your func
		- And it can produce a message to the desired topic

*/

// A Config holds all the configuration of the Kamux class.
type Config struct {
	// Brokers defines the list of kafka brokers to connect to.
	Brokers []string
	// User is the Kafka's user.
	User string
	// Password is the Kafka's password.
	Password string
	// Topics are all the topics on which consumer groups listen.
	Topics []string
	// ConsumerGroup is the name of the consumer group to use.
	ConsumerGroup string
	// Handler is the function executed on each kafka message.
	Handler func(*sarama.ConsumerMessage) error
	// ErrHandler is the function executed on Handler's error used to trying to rescue the error.
	ErrHandler func(error, *sarama.ConsumerMessage) error
	// PreRun is the function executed before the launch on processing.
	PreRun func(*Kamux) error
	// PostRun is the function executed on kamux close.
	PostRun func(*Kamux) error
	// StopOnError, whether or not to stop processing on handler error.
	StopOnError bool
	// MarkOffsets, whether or not to mark offsets on each message processing.
	MarkOffsets bool
	// Debug enables debug mode, more verbose output
	Debug bool
	// MessagesBufferSize is the buffer size of the messages that a worker can queue.
	MessagesBufferSize int
	// ForceKafkaVersion overrides kafka cluster version on sarama library.
	ForceKafkaVersion *sarama.KafkaVersion
	// Logger is used to print some Kamux's information. Golang's log package is used as default.
	Logger Logger
}

// Kamux is the main object
// for the Kamux
type Kamux struct {
	Config         *Config
	ConsumerConfig *sarama.Config

	// Internal stuff
	globalLock    *sync.RWMutex
	kafkaClient   sarama.Client
	kafkaConsumer sarama.ConsumerGroup
	ready         chan bool
	launched      bool
	err           error
	runningWaiter *sync.WaitGroup
}

// NewKamux is the constructor of the ConsumerProducer
// It will make some config checks, and prepare the kafka connections for
// the upcoming launch of the process
func NewKamux(config *Config) (kamux *Kamux, err error) {

	// Check configuration
	if config == nil {
		return nil, errors.New("kamux: configuration object is missing")
	}
	if len(config.Brokers) == 0 {
		return nil, errors.New("kamux: no kafka brokers specified")
	}
	if config.User == "" || config.Password == "" {
		return nil, errors.New("kamux: no kafka user or password specified")
	}
	if len(config.Topics) == 0 {
		return nil, errors.New("kamux: no kafka consuming topics specified")
	}
	if config.Handler == nil {
		return nil, errors.New("kamux: no handler specified")
	}
	if config.MessagesBufferSize == 0 {
		config.MessagesBufferSize = 10000
	}
	if config.Logger == nil {
		config.Logger = log.New(os.Stderr, "", log.LstdFlags)
	}

	// Init object with configuration
	kamux = new(Kamux)
	kamux.Config = config
	kamux.ConsumerConfig = sarama.NewConfig()
	kamux.ConsumerConfig.ChannelBufferSize = config.MessagesBufferSize
	kamux.ConsumerConfig.Net.SASL.Enable = true
	kamux.ConsumerConfig.Net.SASL.User = kamux.Config.User
	kamux.ConsumerConfig.Net.SASL.Password = kamux.Config.Password
	kamux.ConsumerConfig.Net.TLS.Enable = true
	kamux.ConsumerConfig.Consumer.Return.Errors = true
	kamux.globalLock = new(sync.RWMutex)
	kamux.runningWaiter = new(sync.WaitGroup)

	// Force kafka version
	if kamux.Config.ForceKafkaVersion != nil {
		kamux.ConsumerConfig.Version = *kamux.Config.ForceKafkaVersion
	}

	return
}

// Launch will begin the processing of the kafka messages
// It can be launched only once.
// It will :
//  	- Connect to kafka using credentials provided in configuration
//		- Listen to consumer group notifications (rebalance,...)
//		- Listen to consumer errors, and stop properly in case of one
//		- Listen to system SIGINT to stop properly
//
func (kamux *Kamux) Launch() (err error) {

	// Launch only once
	kamux.globalLock.Lock()

	if kamux.launched {
		kamux.globalLock.Unlock()
		return
	}

	// PreRun
	if kamux.Config.PreRun != nil {
		kamux.Config.Logger.Printf("[KAMUX] Executing PreRun function defined in configuration")

		err = kamux.Config.PreRun(kamux)
		if err != nil {
			kamux.Config.Logger.Printf("[KAMUX] Fail to exec PreRun function : %s", err)
			kamux.globalLock.Unlock()
			return err
		}
	}

	// Init kafka client
	kamux.Config.Logger.Printf("[KAMUX] Connecting on kafka on brokers %v with user %s", kamux.Config.Brokers, kamux.ConsumerConfig.Net.SASL.User)
	kamux.kafkaClient, err = sarama.NewClient(kamux.Config.Brokers, kamux.ConsumerConfig)
	if err != nil {
		kamux.globalLock.Unlock()
		return
	}

	// Init kafka consumer
	kamux.Config.Logger.Printf("[KAMUX] Using consumer group %s on topics : %v", kamux.Config.ConsumerGroup, kamux.Config.Topics)
	kamux.kafkaConsumer, err = sarama.NewConsumerGroupFromClient(kamux.Config.ConsumerGroup, kamux.kafkaClient)
	if err != nil {
		kamux.globalLock.Unlock()
		return
	}

	kamux.ready = make(chan bool)
	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			// The "Consume" method should be called inside infinite loop
			// When a rebalance happens we need to recreate the consumer sessions to get the new claims
			// See: https://github.com/Shopify/sarama/blob/master/consumer_group.go#L41
			err := kamux.kafkaConsumer.Consume(ctx, kamux.Config.Topics, kamux)
			if err != nil && err != sarama.ErrClosedConsumerGroup {
				kamux.Config.Logger.Panicf("[KAMUX] Error from consumer: %v", err)
			}

			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				return
			}

			// Reallocating the channel unblocks goroutines waiting for it
			kamux.ready = make(chan bool)
		}
	}()

	// Wait for the consumer to be ready
	<-kamux.ready

	kamux.launched = true
	kamux.globalLock.Unlock()
	kamux.Config.Logger.Printf("[KAMUX] Kamux is now ready. All consumers are started")

	// Handle errors, sigterms and ctx cancellation
	kamux.handleErrorsAndNotifications(ctx)
	cancel()
	wg.Wait()

	kamux.Config.Logger.Printf("[KAMUX] Kamux is now fully stopped")
	return kamux.err
}

// Stop will stop processing with no error
func (kamux *Kamux) Stop() error {
	return kamux.StopWithError(nil)
}

// StopWithError will stop processing
// with error passed as argument
func (kamux *Kamux) StopWithError(err error) error {

	// Launch once
	kamux.globalLock.Lock()
	defer kamux.globalLock.Unlock()

	// Wait for current handler to finish
	kamux.runningWaiter.Wait()

	// Set error
	kamux.err = err

	// Stop consumer: no more messages to be available
	kamux.Config.Logger.Printf("[KAMUX] Closing kafka consumer group")
	if kamux.kafkaConsumer != nil {
		err = kamux.kafkaConsumer.Close()
		if err != nil {
			return err
		}
	}
	kamux.Config.Logger.Printf("[KAMUX] -> Success")

	return nil
}

func (kamux *Kamux) handleErrorsAndNotifications(ctx context.Context) {
	// Listen to SIGINT and SIGTERM
	kamux.Config.Logger.Printf("[KAMUX] Listening for notifications and system signals")
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-kamux.kafkaConsumer.Errors():
		if err != nil {
			kamux.Config.Logger.Printf("[KAMUX] Error on kafka consumer : %s", err)

			err = kamux.StopWithError(err)
			if err != nil {
				kamux.Config.Logger.Printf("[KAMUX] Fail to stop kamux: %s", err)
			}
		}
	case sig := <-sigs:
		kamux.Config.Logger.Printf("[KAMUX] Got a %s signal. Stopping gracefully....", sig)

		err := kamux.Stop()
		if err != nil {
			kamux.Config.Logger.Printf("[KAMUX] Fail to stop kamux: %s", err)
		}
	case <-ctx.Done():
		err := kamux.Stop()
		if err != nil {
			kamux.Config.Logger.Printf("[KAMUX] Fail to stop kamux: %s", err)
		}
	}
}

//
//// Kamux class implements interface sarama.ConsumerGroupHandler
//

// Setup is run at the beginning of a new session, before ConsumeClaim.
func (kamux *Kamux) Setup(sarama.ConsumerGroupSession) (err error) {
	close(kamux.ready) // Mark the consumer as ready
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exites
// but before the offsets are committed for the very last time.
func (kamux *Kamux) Cleanup(sarama.ConsumerGroupSession) (err error) {
	if kamux.Config.PostRun != nil {
		kamux.Config.Logger.Printf("[KAMUX] Executing PostRun function defined in configuration")

		err = kamux.Config.PostRun(kamux)
		if err != nil {
			kamux.Config.Logger.Printf("[KAMUX] Fail to exec PostRun function : %s", err)
			return err
		}
	}

	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
// Once the Messages() channel is closed, the Handler must finish its processing
// loop and exit.
func (kamux *Kamux) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) (err error) {

	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29

	kamux.Config.Logger.Printf("[KAMUX] Begin processing on topic %s and partition %d", claim.Topic(), claim.Partition())

	for message := range claim.Messages() {
		err := func() error {
			kamux.runningWaiter.Add(1)
			defer kamux.runningWaiter.Done()
			// Execute handler
			err = kamux.Config.Handler(message)
			if err != nil {
				if kamux.Config.ErrHandler != nil {
					err = kamux.Config.ErrHandler(err, message)
				}

				// Still error after error handler ?
				if err != nil && kamux.Config.StopOnError {
					return err
				}
			}

			// Mark offset if asked
			if kamux.Config.MarkOffsets {
				session.MarkMessage(message, "")
			}
			return nil
		}()
		if err != nil {
			return kamux.StopWithError(err)
		}
	}

	kamux.Config.Logger.Printf("[KAMUX] Closed processing on topic %s and partition %d", claim.Topic(), claim.Partition())
	return nil
}
