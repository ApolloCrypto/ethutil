package ethutil

import (
	"context"
	"errors"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

const maxWorkerNumber = 50
const defaultHandlerNumberPerWorker = 200
const maxConcurrentHandlerNumber = maxWorkerNumber * defaultHandlerNumberPerWorker

var retryDuration = time.Second

// logs filter Stream

type LogsStream struct {
	client *ethClient
	logs   []types.Log
	err    error
	nocopy nocopy
	//stream mutex
	m          sync.Mutex
	workMutex  sync.Mutex
	work       []*workUnit
	canWork    chan int
	waitWork   []*workUnit
	finishWork []*workUnit
	workNumber int
	init       bool
	group      sync.WaitGroup
}

func (l *LogsStream) check() {
	if uintptr(l.nocopy) != uintptr(unsafe.Pointer(l)) && !atomic.CompareAndSwapUintptr((*uintptr)(&l.nocopy), 0, uintptr(unsafe.Pointer(l))) && uintptr(l.nocopy) != uintptr(unsafe.Pointer(l)) {
		panic(any("object has copied"))
	}
}
func (l *LogsStream) tryNotify() *workUnit {
	l.workMutex.Lock()
	defer l.workMutex.Unlock()
	if len(l.waitWork) < 1 {
		return nil
	}
	<-l.canWork
	var work = l.waitWork[0]
	l.waitWork = l.waitWork[1:]
	l.group.Add(1)
	return work
}
func NewLogsStream(log []types.Log, client *ethClient) *LogsStream {
	return &LogsStream{
		logs:      log,
		client:    client,
		m:         sync.Mutex{},
		workMutex: sync.Mutex{},
		group:     sync.WaitGroup{},
	}
}
func NewDefaultLogsStream(log []types.Log) (*LogsStream, error) {
	if globalProto.client == nil {
		return nil, errors.New("default client cant init")
	}
	return &LogsStream{
		logs:      log,
		client:    globalProto,
		m:         sync.Mutex{},
		workMutex: sync.Mutex{},
		group:     sync.WaitGroup{},
	}, nil
}
func (l *LogsStream) workBefore() {
	onWorkStream(l)
}
func (l *LogsStream) workAfter() {
	collect(l)
}

func (l *LogsStream) TxFromAndTo(from []common.Address) *LogsStream {
	l.check()
	l.m.Lock()
	defer l.m.Unlock()
	if l.err != nil {
		return l
	}
	l.workBefore()
	defer l.workAfter()
	//handler
	for i := 0; i < len(l.work); i++ {
		go func(index int) {
			txFromAndTo(l.work[index], from)
			if !l.work[index].tryEnd() {
				work := l.tryNotify()
				txFromAndTo(work, from)
				work.end()
			}
		}(i)
	}
	l.group.Wait()
	for len(l.waitWork) != 0 {
		work := l.tryNotify()
		if work == nil {
			break
		}
		go func(work *workUnit) {
			txFromAndTo(work, from)
			work.end()
		}(work)
	}
	l.group.Wait()
	return l
}

var txFromAndTo = func(unit *workUnit, from []common.Address) {
filter:
	for i := unit.from; i < unit.to; i++ {
		var logsInfo = unit.stream.logs[i]
	retryFind:
		client := unit.stream.client.GetRawClient()
		hash, b, err := client.TransactionByHash(context.Background(), logsInfo.TxHash)
		if err != nil || b {
			time.Sleep(retryDuration)
			goto retryFind
		}
	retrySearch:
		receipt, err := client.TransactionSender(context.Background(), hash, logsInfo.BlockHash, logsInfo.TxIndex)
		if err != nil {
			time.Sleep(retryDuration)
			goto retrySearch
		}
		for j := 0; j < len(from); j++ {
			var address = from[j]
			if address == receipt {
				unit.result = append(unit.result, logsInfo)
				continue filter
			}
		}
	}
}

func (l *LogsStream) FilterLog(filter FilterFunc) *LogsStream {
	l.check()
	if l.err != nil {
		return l
	}
	l.workBefore()
	defer l.workAfter()
	var eventually error
	for i := 0; i < len(l.work); i++ {
		go func() {
			work := l.work[i]
			err := filter(l.logs[work.from:work.to], work)
			if err != nil && eventually == nil {
				eventually = err
			}
			if !work.tryEnd() {
				notify := l.tryNotify()
				err = filter(l.logs[work.from:work.to], notify)
				if err != nil && eventually == nil {
					eventually = err
				}
				notify.end()
			}
		}()
	}
	l.group.Wait()
	for len(l.waitWork) != 0 {
		work := l.tryNotify()
		if work == nil {
			break
		}
		go func(work *workUnit) {
			err := filter(l.logs[work.from:work.to], work)
			if err != nil && eventually == nil {
				eventually = err
			}
			work.end()
		}(work)
	}
	if eventually != nil {
		l.err = eventually
	}
	return l
}

func (l *LogsStream) Done() (result []types.Log, err error) {
	l.check()
	result = make([]types.Log, len(l.logs))
	copy(result, l.logs)
	defer streamFinalizer(l)
	if l.err != nil {
		err = l.err
		return
	}
	return result, nil
}

func streamFinalizer(l *LogsStream) {
	for i := 0; i < len(l.finishWork); i++ {
		l.finishWork[i].stream = nil
	}
	runtime.SetFinalizer(l, nil)
}

type nocopy uintptr
type FilterFunc func(log []types.Log, work *workUnit) error

func onWorkStream(stream *LogsStream) {
	var workNumber, remainNumber int
	if maxConcurrentHandlerNumber < len(stream.logs) {
		workNumber = maxWorkerNumber
		remainLogs := len(stream.logs) - maxConcurrentHandlerNumber
		remainNumber = remainLogs/defaultHandlerNumberPerWorker + 1
		if (remainNumber-1)*defaultHandlerNumberPerWorker == remainLogs {
			remainNumber = remainNumber - 1
		}
	} else {
		workNumber = len(stream.logs)/defaultHandlerNumberPerWorker + 1
		if (workNumber-1)*defaultHandlerNumberPerWorker == len(stream.logs) {
			workNumber = workNumber - 1
		}
	}
	stream.group.Add(workNumber)
	stream.finishWork = make([]*workUnit, 0, workNumber+remainNumber)
	stream.workNumber = workNumber + remainNumber
	mallocWorkUnit(stream, workNumber, remainNumber)
}
func mallocWorkUnit(stream *LogsStream, workNumber int, remainNumber int) {
	var begin, end = 0, len(stream.logs)
	if !stream.init {
		stream.work = make([]*workUnit, 0, workNumber)
	}
	for i := 0; i < workNumber; i++ {
		if begin > end {
			return
		}
		var to = defaultHandlerNumberPerWorker + begin
		if to > end {
			to = end
		}
		if !stream.init {
			var work = &workUnit{result: make([]types.Log, 0, defaultHandlerNumberPerWorker), from: begin, to: to, stream: stream}
			stream.work = append(stream.work, work)
			stream.finishWork = append(stream.finishWork, work)
		} else {
			var temp = stream.work[i]
			temp.from = begin
			temp.to = to
			temp.result = temp.result[0:0]
		}
		begin = to
	}
	if remainNumber == 0 {
		return
	}
	if !stream.init {
		stream.canWork = make(chan int, remainNumber)
		stream.waitWork = make([]*workUnit, 0, remainNumber)
	}
	for i := 0; i < remainNumber; i++ {
		if begin > end {
			return
		}
		var to = defaultHandlerNumberPerWorker + begin
		if to > end {
			to = end
		}
		if !stream.init {
			var work = &workUnit{result: make([]types.Log, 0, defaultHandlerNumberPerWorker), from: begin, to: to, stream: stream}
			stream.waitWork = append(stream.waitWork, work)
			stream.finishWork = append(stream.finishWork, work)
		} else {
			var temp = stream.finishWork[workNumber+i]
			temp.from = begin
			temp.to = to
			temp.result = temp.result[0:0]
			stream.waitWork = append(stream.waitWork, temp)
		}
		begin = to
	}
}
func collect(stream *LogsStream) {
	stream.logs = nil
	var result = make([]types.Log, 0, len(stream.logs))
	for i := 0; i < stream.workNumber; i++ {
		result = append(result, stream.finishWork[i].result...)
	}
	stream.logs = result
}

type workUnit struct {
	result []types.Log
	from   int
	to     int
	stream *LogsStream
}

func (work *workUnit) tryEnd() bool {
	work.stream.group.Done()
	work.stream.workMutex.Lock()
	defer work.stream.workMutex.Unlock()
	if work.stream.waitWork == nil || len(work.stream.waitWork) == 0 || len(work.stream.canWork) == len(work.stream.waitWork) {
		return true
	}
	work.stream.canWork <- 1
	return false
}
func (work *workUnit) end() {
	work.stream.group.Done()
}
