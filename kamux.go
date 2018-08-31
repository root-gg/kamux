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

func (kamux *Kamux) Launch() (err error) {

	// Launch only once
	kamux.globalLock.Lock()

	if !kamux.launched {

		// Init kafka client
		kamux.kafkaClient, err = cluster.NewClient(kamux.Config.Brokers, kamux.ConsumerConfig)
		if err != nil {
			kamux.globalLock.Unlock()
			return
		}

		// Init kafka consumer
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
	}

	return
}

func (kamux *Kamux) Stop() error {
	return kamux.StopWithError(nil)
}

func (kamux *Kamux) StopWithError(err error) error {

	// Launch once
	kamux.globalLock.Lock()
	defer kamux.globalLock.Unlock()

	// Stop consumer : no more messages to be available
	kamux.kafkaConsumer.Close()

	// Wait for all workers to be fully closed
	log.Printf("[SCP       ] Waiting all workers to finish...")
	kamux.waitGroup.Wait()

	// Return err passed as argument
	log.Printf("[SCP       ] Kamux is now fully stopped")
	return err
}

func (kamux *Kamux) dispatcher() {

	// Iterate on main kafka messages channel
	// and dispatch them to the right worker
	log.Printf("[SCP       ] Listening to kafka messages...")
	for consumerMessage := range kamux.kafkaConsumer.Messages() {
		kamux.DispatchMessage(consumerMessage)
	}

	// No more messages from kafka (channel was closed)
	// We can stop workers
	log.Printf("[SCP       ] No more messages on consumer, closing workers properly...")

	for partition, worker := range kamux.workers {
		log.Printf("[SCP       ] Closing worker on partition %d", partition)
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
				log.Printf("[SCP       ] Error on kafka consumer : %s", err)
				kamux.StopWithError(err)
				return
			}

		case notif := <-kamux.kafkaConsumer.Notifications():
			if notif != nil {
				log.Printf("[SCP       ] Notification: %s on kafka consumer", notif.Type.String())
			}

		case sig := <-sigs:
			log.Printf("[SCP       ] Got a %s signal. Stopping gracefully....", sig)
			kamux.Stop()
			return

		}
	}
}

func (kamux *Kamux) DispatchMessage(consumerMessage *sarama.ConsumerMessage) {

	// Create worker if it does not exists yet
	if kamux.workers[consumerMessage.Partition] == nil {
		kamux.workers[consumerMessage.Partition] = NewKamuxWorker(kamux)
		kamux.waitGroup.Add(1)
	}

	// Enqueue message in the partition worker
	kamux.workers[consumerMessage.Partition].Enqueue(consumerMessage)
}
