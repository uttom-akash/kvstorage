package lsmtree

import (
	"pkvstore/internal/storageengine/configs"
	"pkvstore/internal/storageengine/memtable"
	"pkvstore/internal/storageengine/sstable"
	"sort"
)

func (lsm *LSMTree) listenForflush() {
	for oldMemtable := range lsm.backgroundChan.NewMutationEventChannel {

		sst := createSsTableFromMemtable(oldMemtable)

		lsm.sstTables[sst.Header.Level] = append(lsm.sstTables[sst.Header.Level], sst)

		lsm.doCompaction()
	}
}

func (lsm *LSMTree) doCompaction() {

	configs := configs.GetStorageEngineConfig()

	for index := configs.LSMTreeConfig.FirstLevel; index >= 0; index-- {
		if len(lsm.sstTables[index]) <= 1<<index {
			break
		}

		if index > 0 { //tiered
			mergedSSTable := mergeSSTables(lsm.sstTables[index], uint8(index)-1)
			lsm.sstTables[index] = make([]*sstable.SSTable, 0)
			lsm.sstTables[index-1] = append(lsm.sstTables[index-1], mergedSSTable)
		} else { //leveled
			mergedSSTable := mergeSSTables(lsm.sstTables[index], uint8(index))
			lsm.sstTables[index] = []*sstable.SSTable{mergedSSTable}
		}
	}
}

func mergeSSTables(sstables []*sstable.SSTable, newLevel uint8) *sstable.SSTable {

	keyValues := make(map[string]*sstable.SSTableEntry)

	//deduplication
	for _, ssTable := range sstables {
		for _, block := range ssTable.Blocks {
			for _, entry := range block.Entries {
				keyValues[entry.Key] = entry
			}
		}
	}

	// list entries
	var mergedSSTableEntries []*sstable.SSTableEntry
	for _, sstableEntries := range keyValues {
		mergedSSTableEntries = append(mergedSSTableEntries, sstableEntries)
	}

	sort.Slice(mergedSSTableEntries, func(i, j int) bool {
		return mergedSSTableEntries[i].Key < mergedSSTableEntries[j].Key
	})

	sst := sstable.CreateSSTable(mergedSSTableEntries, newLevel)

	return sst
}

func createSsTableFromMemtable(memTable *memtable.MemTable) *sstable.SSTable {

	sstableEntries := make([]*sstable.SSTableEntry, 0)

	configs := configs.GetStorageEngineConfig()

	for k, v := range memTable.Table {
		sstableEntries = append(sstableEntries, sstable.NewSSTableEntry(k, v.Value, v.IsTombstone))
	}

	sst := sstable.CreateSSTable(sstableEntries, uint8(configs.LSMTreeConfig.FirstLevel))

	return sst
}
