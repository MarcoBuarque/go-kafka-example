package producer

import "fmt"

type MockProducerClient struct {
	ShouldError bool
}

func (mpc MockProducerClient) Close() {}

func (mpc MockProducerClient) PostMessage(key string, message []byte, topic string) error {
	if mpc.ShouldError {
		return fmt.Errorf("dummy error")
	}

	return nil
}
