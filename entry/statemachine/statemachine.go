package statemachine

import (
	"github.com/syndtr/goleveldb/leveldb"
	zlog "github.com/zhangyu0310/zlogger"
)

var S *StateMachine

type StateMachine struct {
	*leveldb.DB
}

func InitStateMachine() {
	// TODO: Path can be config.
	db, err := leveldb.OpenFile("./state_machine", nil)
	if err != nil {
		zlog.Panic("Init state_machine failed.", err)
	}
	S = &StateMachine{
		db,
	}
}

func Close() {
	err := S.Close()
	if err != nil {
		zlog.Error("Close state machine failed.", err)
	}
}
