package producer

type IProducerCli interface {
	Stop()
	Post(message []byte, key, topic string) error
}
