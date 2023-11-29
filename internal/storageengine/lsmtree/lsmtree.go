package lsmtree

import (
	"pkvstore/internal/models"
	"pkvstore/internal/storageengine/configs"
	"pkvstore/internal/storageengine/memtable"
	"pkvstore/internal/storageengine/sstable"
)

type LSMTree struct {
	MemTable *memtable.MemTable
	SSTables [][]*sstable.SSTable
}

func NewLSMTree() *LSMTree {
	config := configs.GetStorageEngineConfig()

	sstables := make([][]*sstable.SSTable, config.LSMTreeConfig.NumberOfSSTableLevels)

	lsmTree := &LSMTree{
		MemTable: memtable.NewMemTable(),
		SSTables: sstables,
	}

	return lsmTree
}

func (lsm *LSMTree) Get(key string) *models.Result {

	// complexity
	// level = 6
	// total 63 sstables
	// last level = 10^9 keys
	// block cap = 2048
	// blocks = 10^5
	// so overall compelxity = binary search - log2(10^5)

	result := lsm.MemTable.Get(key)

	if result.Status == models.Found || result.Status == models.Deleted {
		return result
	}

	config := configs.GetStorageEngineConfig()

	for level := config.LSMTreeConfig.FirstLevel; level >= config.LSMTreeConfig.LastLevel; level-- {
		for sstableId := len(lsm.SSTables[level]) - 1; sstableId >= 0; sstableId-- {
			currentSSTable := lsm.SSTables[level][sstableId]

			if currentSSTable.DoesNotExist(key) {
				continue
			}

			result = currentSSTable.ReadFromSSTable(key)

			if result.Status == models.Found || result.Status == models.Deleted {
				return result
			}
		}
	}

	return models.NewNotFoundResult()
}

func (lsm *LSMTree) Put(key, value string) {
	lsm.MemTable.Put(key, value)
}

func (lsm *LSMTree) Delete(key string) {
	lsm.MemTable.Delete(key)
}
