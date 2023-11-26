package lsmtree

import (
	"pkvstore/internal/models"
	"pkvstore/internal/storageengine/configs"
	"pkvstore/internal/storageengine/memtable"
	"pkvstore/internal/storageengine/sstable"
	"sort"
)

type LSMTree struct {
	memTable  *memtable.MemTable
	sstTables [][]*sstable.SSTable
}

func NewLSMTree() *LSMTree {

	config := configs.GetStorageEngineConfig()

	var sstables [][]*sstable.SSTable

	for index := 0; index < config.LSMTreeConfig.SSTableLevels; index++ {
		sstables = append(sstables, make([]*sstable.SSTable, 0))
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

		lsm.flushAsSSTable(oldMemtable)
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

// private

func (lsm *LSMTree) flushAsSSTable(oldMemtable *memtable.MemTable) {

	sst := sstable.CreateSsTableFromMemtable(oldMemtable)

	lsm.sstTables[sst.Header.Level] = append(lsm.sstTables[sst.Header.Level], sst)

	lsm.doCompaction()
	//sst.WriteToFile(sst.GetFileName())
}

func (lsm *LSMTree) doCompaction() {

	for index := 6; index >= 0; index-- {
		if len(lsm.sstTables[index]) <= 1<<index {
			break
		}

		mergedSSTable := mergeSSTables(lsm.sstTables[index])

		if index > 0 {
			lsm.sstTables[index] = make([]*sstable.SSTable, 0)
			lsm.sstTables[index-1] = append(lsm.sstTables[index-1], mergedSSTable)
		} else {
			lsm.sstTables[index] = []*sstable.SSTable{mergedSSTable}
		}
	}
}

func mergeSSTables(sstables []*sstable.SSTable) *sstable.SSTable {

	keyValues := make(map[string]*sstable.SSTableEntry)

	for i := len(sstables) - 1; i >= 0; i-- {
		for _, kv := range sstables[i].Blocks[len(sstables[i].Blocks)-1].Entries {

			keyValues[kv.Key] = sstable.NewSSTableEntry(kv.Key, kv.Value, kv.IsTombstone)
		}
	}

	var mergedData []*sstable.SSTableEntry
	for _, value := range keyValues {
		mergedData = append(mergedData, value)
	}

	sort.Slice(mergedData, func(i, j int) bool {
		return mergedData[i].Key < mergedData[j].Key
	})

	sst := sstable.NewSSTable(sstables[0].Header.Level - 1)

	for _, entry := range mergedData {
		sst.AddEntry(entry.Key, entry.Value, entry.IsTombstone)
	}

	return sst
}
