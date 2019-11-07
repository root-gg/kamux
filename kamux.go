package kamux

import (
	"errors"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	cron "github.com/robfig/cron/v3"
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

// Config is the configuration of the Kamux class.
//
// Brokers : List of kafka brokers to connect to
// User : Kafka user
// Password : Kafka password
// Topics : List of topics to get messages
// ConsumerGroup : Name of the consumer group to use
// Handler : Function executed on each kafka message
// ErrHandler : Function executed on Handler's error used to trying to rescue the error
// PreRun : Function executed before the launch on processing
// PostRun : Function executed on kamux close
// StopOnError : Whether or not to stop processing on handler error
// MarkOffsets : Whether or not to mark offsets on each message processing
// Debug : Enable debug mode, more verbose output
//
type Config struct {
	Brokers           []string
	User              string
	Password          string
	Topics            []string
	ConsumerGroup     string
	Handler           func(*sarama.ConsumerMessage) error
	ErrHandler        func(error, *sarama.ConsumerMessage) error
	PreRun            func(*Kamux) error
	PostRun           func(*Kamux) error
	StopOnError       bool
	MarkOffsets       bool
	Debug             bool
	RemoveIdleWorkers bool
}

// Kamux is the main object
// for the Kamux
type Kamux struct {
	Config         *Config
	ConsumerConfig *cluster.Config

	// Internal stuff
	globalLock    *sync.RWMutex
	kafkaClient   *cluster.Client
	kafkaConsumer *cluster.Consumer
	workers       map[int32]*KamuxWorker
	waitGroup     *sync.WaitGroup
	launched      bool
	err           error
}

// NewKamux is the constructor of the ConsumerProducer
// It will make some config checks, and prepare the kafka connections for
// the upcoming launch of the process
func NewKamux(config *Config) (kamux *Kamux, err error) {

	// Check configuration
	if config == nil {
		return nil, errors.New("Kamux: configuration object is missing")
	}
	if len(config.Brokers) == 0 {
		return nil, errors.New("Kamux: no kafka brokers specified")
	}
	if config.User == "" || config.Password == "" {
		return nil, errors.New("Kamux: no kafka user or password specified")
	}
	if len(config.Topics) == 0 {
		return nil, errors.New("Kamux: no kafka consuming topics specified")
	}
	if config.Handler == nil {
		return nil, errors.New("Kamux: no handler specified")
	}

	// Init object with configuration
	kamux = new(Kamux)
	kamux.Config = config
	kamux.ConsumerConfig = cluster.NewConfig()
	kamux.ConsumerConfig.Net.SASL.Enable = true
	kamux.ConsumerConfig.Net.SASL.User = kamux.Config.User
	kamux.ConsumerConfig.Net.SASL.Password = kamux.Config.Password
	kamux.ConsumerConfig.Net.TLS.Enable = true
	kamux.ConsumerConfig.Consumer.Return.Errors = true
	kamux.ConsumerConfig.Group.Return.Notifications = true
	kamux.globalLock = new(sync.RWMutex)
	kamux.workers = make(map[int32]*KamuxWorker)
	kamux.waitGroup = new(sync.WaitGroup)

	return
}

// Launch will begin the processing of the kafka messages
// It can be launched only once.
// It will :
//  	- Connect to kafka using credentials provided in configuration
//		- Listen to consumer group notifications (rebalance,...)
//		- Listen to consumer errors, and stop properly in case of one
//		- Listen to system SIGINT to stop properly
//		- Dispatch kafka messages on different workers (1 worker per partition)
//
func (kamux *Kamux) Launch() (err error) {

	// Launch only once
	kamux.globalLock.Lock()

	if !kamux.launched {

		// PreRun
		if kamux.Config.PreRun != nil {

			log.Printf("[KAMUX     ] Executing PreRun function defined in configuration")

			err = kamux.Config.PreRun(kamux)
			if err != nil {
				log.Printf("[KAMUX     ] Fail to exec PreRun function : %s", err)
				return err
			}
		}

		// Init kafka client
		log.Printf("[KAMUX     ] Connecting on kafka on brokers %v with user %s", kamux.Config.Brokers, kamux.ConsumerConfig.Net.SASL.User)
		kamux.kafkaClient, err = cluster.NewClient(kamux.Config.Brokers, kamux.ConsumerConfig)
		if err != nil {
			kamux.globalLock.Unlock()
			return
		}

		// Init kafka consumer
		log.Printf("[KAMUX     ] Using consumer group %s on topics : %v", kamux.Config.ConsumerGroup, kamux.Config.Topics)
		kamux.kafkaConsumer, err = cluster.NewConsumerFromClient(kamux.kafkaClient, kamux.Config.ConsumerGroup, kamux.Config.Topics)
		if err != nil {
			kamux.globalLock.Unlock()
			return
		}

		// Setup cron routines
		c := cron.New()
		c.AddFunc("@every 1m", kamux.Stats)
		if kamux.Config.RemoveIdleWorkers {
			c.AddFunc("@every 5m", kamux.removeIdleWorkers)
		}
		c.Start()

		go kamux.handleErrorsAndNotifications()
		kamux.launched = true
		kamux.globalLock.Unlock()

		// Listen events
		err = kamux.dispatcher()
		if err != nil {
			return
		}

		// Wait for all workers to be fully closed
		kamux.waitGroup.Wait()
		log.Printf("[KAMUX     ] Kamux is now fully stopped")

		// Return global kamux err
		return kamux.err
	}

	return
}

// MarkOffset exposal of kamux kafka consumer
// Could be useful if you want to do extra stuff beside handler
func (kamux *Kamux) MarkOffset(msg *sarama.ConsumerMessage, metadata string) {
	kamux.kafkaConsumer.MarkOffset(msg, metadata)
	return
}

// Stats will output a summary of the Kamux state
func (kamux *Kamux) Stats() {

	kamux.globalLock.Lock()
	defer kamux.globalLock.Unlock()

	log.Printf("[KAMUX     ] Kamux live statistics : ")

	totalProcessed := int64(0)
	totalSpeed := int64(0)

	for partition, worker := range kamux.workers {

		totalProcessed += worker.MessagesProcessed()
		totalSpeed += worker.MessagesPerSecond()
		log.Printf("[KAMUX     ]  - Worker %d	: %d events/s (total proccessed : %d)", partition, worker.MessagesPerSecond(), worker.MessagesProcessed())
	}

	log.Printf("[KAMUX     ] Total messages processed : %d", totalProcessed)
	log.Printf("[KAMUX     ] Processing : %d messages/s", totalSpeed)
	return
}

// MarkOffsets exposal of kamux kafka consumer
// Could be useful if you want to do extra stuff beside handler
func (kamux *Kamux) MarkOffsets(s *cluster.OffsetStash) {
	kamux.kafkaConsumer.MarkOffsets(s)
	return
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

	// Set error
	kamux.err = err

	// Stop consumer : no more messages to be available
	err = kamux.kafkaConsumer.Close()
	if err != nil {
		return err
	}

	return nil
}

func (kamux *Kamux) dispatcher() (err error) {

	// Iterate on main kafka messages channel
	// and dispatch them to the right worker
	log.Printf("[KAMUX     ] Listening to kafka messages...")
	for consumerMessage := range kamux.kafkaConsumer.Messages() {
		kamux.dispatchMessage(consumerMessage)
	}

	// No more messages from kafka (channel was closed)
	// We can stop workers
	log.Printf("[KAMUX     ] No more messages on consumer, closing workers properly...")

	for partition, worker := range kamux.workers {
		log.Printf("[KAMUX     ] Closing worker on partition %d", partition)
		worker.Stop()
	}

	// Exec PostRun
	if kamux.Config.PostRun != nil {

		log.Printf("[KAMUX     ] Executing PostRun function defined in configuration")

		err = kamux.Config.PostRun(kamux)
		if err != nil {
			log.Printf("[KAMUX     ] Fail to exec PostRun function : %s", err)
			return err
		}
	}

	return
}

func (kamux *Kamux) handleErrorsAndNotifications() {

	// Listen to SIGINT
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT)

	// Event loop
	for {
		select {

		case err := <-kamux.kafkaConsumer.Errors():
			if err != nil {
				log.Printf("[KAMUX     ] Error on kafka consumer : %s", err)

				err = kamux.StopWithError(err)
				if err != nil {
					log.Printf("[KAMUX     ] Fail to stop kamux: %s", err)
				}

				return
			}

		case notif := <-kamux.kafkaConsumer.Notifications():
			if notif != nil {
				kamux.handleNotification(notif)
			}

		case sig := <-sigs:
			log.Printf("[KAMUX     ] Got a %s signal. Stopping gracefully....", sig)

			err := kamux.Stop()
			if err != nil {
				log.Printf("[KAMUX     ] Fail to stop kamux: %s", err)
			}

			return

		}
	}
}

func (kamux *Kamux) handleNotification(notif *cluster.Notification) {

	switch notif.Type {
	case cluster.RebalanceStart:
		log.Printf("[KAMUX     ] Rebalance started on this consumer")

	case cluster.RebalanceOK:
		log.Printf("[KAMUX     ] Rebalance finished on this consumer :")

		// Log claimed topics/partition
		for topic, partitions := range notif.Claimed {
			if len(partitions) > 0 {
				log.Printf("[KAMUX     ] Gained partitions %v on topic %-20s", partitions, topic)
			}
		}

		// Log released topics/partition
		for topic, partitions := range notif.Released {
			if len(partitions) > 0 {
				log.Printf("[KAMUX     ] Lost partitions   %v on topic %-20s", partitions, topic)
			}
		}

		// Log released topics/partition
		log.Printf("[KAMUX     ] Reminder, we currently manage :")

		for topic, partitions := range notif.Current {
			if len(partitions) > 0 {
				log.Printf("[KAMUX     ] - Partitions %v on topic %-20s", partitions, topic)
			}
		}

	case cluster.RebalanceError:
		log.Printf("[KAMUX     ] Rebalance failed on this consumer")

	}

	return
}

func (kamux *Kamux) dispatchMessage(consumerMessage *sarama.ConsumerMessage) {

	// Create worker if it does not exists yet
	if kamux.workers[consumerMessage.Partition] == nil {
		kamux.workers[consumerMessage.Partition] = NewKamuxWorker(kamux)
		kamux.waitGroup.Add(1)
	}

	// Enqueue message in the partition worker
	kamux.workers[consumerMessage.Partition].Enqueue(consumerMessage)
}

// removeIdleWorkers will stop workers which
// did not process messages since long time
func (kamux *Kamux) removeIdleWorkers() {

	for partition, worker := range kamux.workers {

		if time.Since(worker.LastMessageDate()) > time.Minute {

			kamux.globalLock.Lock()

			// Stop worker
			worker.Stop()

			// Remove it from kamux
			log.Printf("[KAMUX     ] Removing idle worker on partition %d", partition)
			delete(kamux.workers, partition)

			kamux.globalLock.Unlock()
		}
	}

	return
}
