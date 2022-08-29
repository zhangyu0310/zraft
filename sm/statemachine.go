package statemachine

type Batch interface {
	Put(key, value []byte)
	Delete(key []byte)
}

type StateMachine interface {
	Close() error
	Get([]byte, ...interface{}) ([]byte, error)
	Put([]byte, []byte, ...interface{}) error
	Write(*Batch, ...interface{}) error
}
