package ethutil

import (
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"testing"
	"time"
)

func testGetLogs(t *testing.T) {
	//number, _ := GetCurrentBlockNumber()
	err := Init("https://polygon-mainnet.g.alchemy.com/v2/bG72w682SlQ0uQwUcSfbZmQAB1VPwpZd")
	if err != nil {
		return
	}
	event, err := GetClient().GetEvent(time.Duration(TimeLess), 39000000, 39791976, []common.Address{common.HexToAddress("0xC7728354f9fe0e43514B1227162D5B0E40FaD410")}, nil)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	done, err := event.Done()

	if err != nil {
		return
	}
	fmt.Println(len(done))
}
