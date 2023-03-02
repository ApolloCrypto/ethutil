package ethutil

import (
	"github.com/ethereum/go-ethereum/ethclient"
	"sync"
)

var (
	//onlyInit first successfully
	//后面做多条链的单例
	globalProto *ethClient
	once        sync.Once
)

type ethClient struct {
	client *ethclient.Client
}

func (c *ethClient) GetRawClient() *ethclient.Client {
	return c.client
}

func NewEthClient(url string) (*ethClient, error) {
	client, err := ethclient.Dial(url)
	if err != nil {
		return nil, err
	}
	return &ethClient{client: client}, nil
}
func Init(url string) error {
	client, err := ethclient.Dial(url)
	if err != nil {
		return err
	}
	once.Do(func() {
		globalProto = &ethClient{client: client}
	})
	return nil
}

func GetClient() *ethClient {
	return globalProto
}
