package zraft

import "testing"

func TestGet(t *testing.T) {
	_, _ = Get([]byte("aaa"))
}

func TestPut(t *testing.T) {

}

func TestDelete(t *testing.T) {
	_ = Delete([]byte("aaa"))
}

func TestGetLocal(t *testing.T) {

}
