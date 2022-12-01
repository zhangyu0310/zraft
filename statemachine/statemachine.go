package statemachine

import (
	"errors"
	"strings"

	zlog "github.com/zhangyu0310/zlogger"

	"zraft/statemachine/ldbstatemachine"
	"zraft/statemachine/zstatemachine"
)

var (
	ErrUnknownStateMachineEngine = errors.New("unknown state machine engine")
)

type Batch interface {
	Put(key, value []byte)
	Delete(key []byte)
}

type StateMachine interface {
	Close() error
	CreateBatch() Batch
	Get([]byte, interface{}) ([]byte, error)
	Put([]byte, []byte, interface{}) error
	Delete([]byte, interface{}) error
	Write(Batch, interface{}) error
	EngineName() string
}

var s StateMachine

func InitStateMachine(engineName string) error {
	var err error
	switch strings.ToLower(engineName) {
	case strings.ToLower(ldbstatemachine.EngineName()):
		s, err = ldbstatemachine.NewStateMachine()
	case strings.ToLower(zstatemachine.EngineName()):
		s, err = zstatemachine.NewStateMachine()
	default:
		zlog.ErrorF("Unknown state machine engine [%s]", engineName)
		return ErrUnknownStateMachineEngine
	}
	if err != nil {
		zlog.ErrorF("Create state machine [%s] failed, err: %s", engineName, err)
	}
	return err
}

func CreateBatch() Batch {
	return s.CreateBatch()
}

func GetStateMachine() StateMachine {
	return s
}

func Close() {
	err := s.Close()
	if err != nil {
		zlog.Error("Close state machine failed.", err)
	}
}
