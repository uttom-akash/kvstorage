package backgroundprocess

import (
	"pkvstore/internal/storageengine/channels"
	"pkvstore/internal/storageengine/configs"
	"pkvstore/internal/storageengine/lsmtree"
	"pkvstore/internal/storageengine/memtable"
	"pkvstore/internal/storageengine/sstable"
	"sort"
)

type Compaction struct {
	lsmTree    *lsmtree.LSMTree
	sharedChan *channels.SharedChannel
}

func NewCompaction(lsmtree *lsmtree.LSMTree) *Compaction {
	return &Compaction{
		lsmTree:    lsmtree,
		sharedChan: channels.GetSharedChannel(),
	}
}

func (compaction *Compaction) ListenToCompact() {
	for event := range compaction.sharedChan.NewMutationEventChannel {

		if event < 1 {
			continue
		}

		if !compaction.lsmTree.MemTable.ShouldFlush() {
			continue
		}

		compaction.flushMemtable()
		compaction.tryCompactionProcess()
	}
}

func (compaction *Compaction) flushMemtable() {
	oldMemtable := compaction.lsmTree.MemTable.ReplaceNewTable()
	newSSTable := createSsTableFromMemtable(oldMemtable)
	compaction.lsmTree.SSTables[newSSTable.Header.Level] = append(compaction.lsmTree.SSTables[newSSTable.Header.Level], newSSTable)
}

func (compaction *Compaction) tryCompactionProcess() {

	configs := configs.GetStorageEngineConfig()

	for level := configs.LSMTreeConfig.FirstLevel; level >= 0; level-- {
		if len(compaction.lsmTree.SSTables[level]) <= 1<<level {
			break
		}

		if level > 0 { //tiered

			mergedSSTable := mergeSSTables(compaction.lsmTree.SSTables[level], uint8(level)-1)
			compaction.lsmTree.SSTables[level] = make([]*sstable.SSTable, 0)
			compaction.lsmTree.SSTables[level-1] = append(compaction.lsmTree.SSTables[level-1], mergedSSTable)

		} else { //leveled

			mergedSSTable := mergeSSTables(compaction.lsmTree.SSTables[level], uint8(level))
			compaction.lsmTree.SSTables[level] = []*sstable.SSTable{mergedSSTable}

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

func createSsTableFromMemtable(memTable *memtable.FlushableMemTable) *sstable.SSTable {

	sstableEntries := make([]*sstable.SSTableEntry, 0)

	configs := configs.GetStorageEngineConfig()

	for k, v := range memTable.Table {
		sstableEntries = append(sstableEntries, sstable.NewSSTableEntry(k, v.Value, v.IsTombstone))
	}

	sst := sstable.CreateSSTable(sstableEntries, uint8(configs.LSMTreeConfig.FirstLevel))

	return sst
}
