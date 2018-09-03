package kamux

import (
	"errors"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
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

// Config is the configuration
// of the Kamux class.
type Config struct {
	Brokers       []string
	User          string
	Password      string
	Topics        []string
	ConsumerGroup string
	Handler       func(*sarama.ConsumerMessage) error
	StopOnError   bool
	MarkOffsets   bool
	Debug         string
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

		go kamux.handleErrorsAndNotifications()
		kamux.launched = true
		kamux.globalLock.Unlock()

		// Listen events
		kamux.dispatcher()

		// Wait for all workers to be fully closed
		log.Printf("[KAMUX     ] Waiting all workers to finish...")
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

func (kamux *Kamux) dispatcher() {

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
