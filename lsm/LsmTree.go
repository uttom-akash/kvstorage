package lsm

import (
	"pkvstore/memtable"
	"pkvstore/models"
	"pkvstore/sstable"
)

const NUMBER_LEVELS = 7 // sstables: first level = 2^6, last level = 2^0

type LSMTree struct {
	memTable  *memtable.MemTable
	sstTables [NUMBER_LEVELS][]*sstable.SSTable
}

func NewLSMTree() *LSMTree {

	var sstables [NUMBER_LEVELS][]*sstable.SSTable

	for index := range sstables {
		sstables[index] = make([]*sstable.SSTable, 0)
	}

	return &LSMTree{
		memTable:  memtable.NewMemTable(),
		sstTables: sstables,
	}
}

func (lsm *LSMTree) Put(key string, value string) {

	lsm.memTable.Put(key, value)

	if lsm.memTable.ShouldFlush() {
		oldMemtable := lsm.memTable

		lsm.memTable = memtable.NewMemTable()

		lsm.FlushAsSSTable(oldMemtable)
	}
}

func (lsm *LSMTree) FlushAsSSTable(oldMemtable *memtable.MemTable) {

	sst := sstable.CreateSsTableFromMemtable(oldMemtable)

	lsm.sstTables[sstable.FirstLevel] = append(lsm.sstTables[sstable.FirstLevel], sst)

	lsm.DoCompaction()
	//sst.WriteToFile(sst.GetFileName())
}

func (lsm *LSMTree) DoCompaction() {

	for index := 6; index >= 0; index-- {
		if len(lsm.sstTables[index]) <= 1<<index {
			break
		}

		mergedSSTable := sstable.MergeSSTables(lsm.sstTables[index])

		if index > 0 {
			lsm.sstTables[index] = make([]*sstable.SSTable, 0)
			lsm.sstTables[index-1] = append(lsm.sstTables[index-1], mergedSSTable)
		} else {
			lsm.sstTables[index] = []*sstable.SSTable{mergedSSTable}
		}
	}
}

func (lsm *LSMTree) Get(key string) *models.Result {

	result := lsm.memTable.Get(key)

	if result.Status == models.Found || result.Status == models.Deleted {
		return result
	}

	for i := len(lsm.sstTables) - 1; i >= 0; i-- {

		for j := len(lsm.sstTables[i]) - 1; j >= 0; j-- {
			if lsm.sstTables[i][j].DoesNotExist(key) {
				continue
			}

			result = lsm.sstTables[i][j].ReadFromSSTable(key)

			if result.Status == models.Found || result.Status == models.Deleted {
				return result
			}
		}
	}

	return models.NewNotFoundResult()
}

func (lsm *LSMTree) Delete(key string) bool {

	lsm.memTable.Delete(key)

	return true
}
