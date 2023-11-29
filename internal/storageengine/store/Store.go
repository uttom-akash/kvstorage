package store

import (
	"pkvstore/internal/models"
	"pkvstore/internal/storageengine/backgroundprocess"
	"pkvstore/internal/storageengine/channels"
	"pkvstore/internal/storageengine/lsmtree"
)

type Store struct {
	lsmTree    *lsmtree.LSMTree
	compaction *backgroundprocess.Compaction
	sharedChan *channels.SharedChannel
}

func NewStore() *Store {
	lsm := lsmtree.NewLSMTree()
	compaction := backgroundprocess.NewCompaction(lsm)
	go compaction.ListenToCompact()

	return &Store{
		lsmTree:    lsm,
		compaction: compaction,
		sharedChan: channels.GetSharedChannel(),
	}
}

func (store *Store) Get(key string) *models.Result {

	result := store.lsmTree.Get(key)

	store.notifyReadOperation()

	return result
}

func (store *Store) Put(key, value string) {

	store.lsmTree.Put(key, value)

	store.notifyWriteOperation()
}

func (store *Store) Delete(key string) {

	store.lsmTree.Delete(key)

	store.notifyWriteOperation()
}

func (store *Store) notifyWriteOperation() {

	store.sharedChan.NewMutationEventChannel <- 1
}

func (store *Store) notifyReadOperation() {

}
