package lsmtree

import "pkvstore/internal/storageengine/memtable"

type SharedChannel struct {
	NewMutationEventChannel chan *memtable.MemTable
}

func NewSharedChannel() *SharedChannel {
	return &SharedChannel{
		NewMutationEventChannel: make(chan *memtable.MemTable, 1000),
	}
}
