package ethereum_util

import "github.com/ethereum/go-ethereum/core/types"

// logs filter Stream

type LogsStream struct {
	logs []types.Log
	err  error
}
type FilterFunc func([]types.Log) error

func NewLogsStream(log []types.Log) *LogsStream {
	return &LogsStream{logs: log}
}

func (l *LogsStream) FilterLog(filter FilterFunc) *LogsStream {
	if l.err != nil {
		return l
	}
	err := filter(l.logs)
	if err != nil {
		l.err = err
	}
	return l
}
func (l *LogsStream) Done() (logs []types.Log, err error) {
	if l.err != nil {
		return nil, l.err
	}
	return l.logs, nil
}
