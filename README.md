Kamux
============

Kamux is a simple library to interact with a Kafka cluster.

It can:
  * Connect with a kafka cluster (Only SASL/TLS with user/password)
  * Listen to one or multiple topics on a specific consumer group
  * Execute the function of your choice (handler)
  * Mark offset or not after handling
  * Listen to SIGINT and stop processing messages gracefully
  * 



A simple example :

~~~golang
func main() {

    config := &kamux.Config{
        Brokers:       []string{"kafka.broker1.net:9093", "kafka.broker2.net:9093"},
        User:          "myKafkaUser",
        Password:      "myPassword",
        Topics:        []string{"myTopic"},
        ConsumerGroup: "myConsumerGroup",
        Handler:       Handler,
        MarkOffsets:    true,
    }

    kamux, err := kamux.NewKamux(config)
    if err != nil {
        log.Fatalf("Fail to init kamux: %s", err)
    }

    kamux.Launch()
}

func Handler(sm *sarama.ConsumerMessage) ( err error ) {
    log.Printf("Received a message on topic : %s (partition: %d | offset: %d)", sm.Topic, sm.Partition, sm.Offset)
    return
}
~~~
