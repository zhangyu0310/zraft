package zstatemachine

import (
	"zraft/statemachine"
	"zraft/statemachine/zstatemachine/zdb"
)

type ZStateMachine struct {
	*zdb.DB
}

func NewStateMachine(_ ...interface{}) (statemachine.StateMachine, error) {
	db, err := zdb.OpenDB(nil)
	if err != nil {
		return nil, err
	}
	sm := &ZStateMachine{db}
	return sm, err
}

func EngineName() string {
	return "ZDB"
}

func (s *ZStateMachine) Close() error {
	return s.DB.Close()
}

func (s *ZStateMachine) CreateBatch() statemachine.Batch {
	return &zdb.Batch{}
}

func (s *ZStateMachine) Get(key []byte, _ interface{}) ([]byte, error) {
	return s.DB.Get(key)
}

func (s *ZStateMachine) Put(key []byte, value []byte, op interface{}) error {
	return s.DB.Put(key, value, op.(*zdb.WriteOptions))
}

func (s *ZStateMachine) Delete(key []byte, op interface{}) error {
	return s.DB.Delete(key, op.(*zdb.WriteOptions))
}

func (s *ZStateMachine) Write(batch statemachine.Batch, op interface{}) error {
	return s.DB.Write(batch.(*zdb.Batch), op.(*zdb.WriteOptions))
}

func (s *ZStateMachine) EngineName() string {
	return EngineName()
}
