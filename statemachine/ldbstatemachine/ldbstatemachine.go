package ldbstatemachine

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	zlog "github.com/zhangyu0310/zlogger"

	"zraft/statemachine"
)

type LdbStateMachine struct {
	*leveldb.DB
}

func NewStateMachine(_ ...interface{}) (statemachine.StateMachine, error) {
	// TODO: Path can be config.
	db, err := leveldb.OpenFile("./state_machine", nil)
	if err != nil {
		zlog.Error("Init state_machine failed.", err)
		return nil, err
	}
	return &LdbStateMachine{db}, nil
}

func EngineName() string {
	return "LevelDB"
}

func (s *LdbStateMachine) Close() error {
	return s.DB.Close()
}

func (s *LdbStateMachine) CreateBatch() statemachine.Batch {
	return &leveldb.Batch{}
}

func (s *LdbStateMachine) Get(key []byte, option interface{}) ([]byte, error) {
	return s.DB.Get(key, option.(*opt.ReadOptions))
}

func (s *LdbStateMachine) Put(key []byte, value []byte, option interface{}) error {
	return s.DB.Put(key, value, option.(*opt.WriteOptions))
}

func (s *LdbStateMachine) Delete(key []byte, option interface{}) error {
	return s.DB.Delete(key, option.(*opt.WriteOptions))
}

func (s *LdbStateMachine) Write(batch statemachine.Batch, option interface{}) error {
	return s.DB.Write(batch.(*leveldb.Batch), option.(*opt.WriteOptions))
}

func (s *LdbStateMachine) EngineName() string {
	return EngineName()
}
