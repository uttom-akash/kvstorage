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
		if event < 1 || !compaction.lsmTree.MemTable.ShouldFlush() {
			continue
		}

		compaction.flushMemtable()
		compaction.tryCompactionProcess()
	}
}

func (compaction *Compaction) flushMemtable() {
	oldMemtable := compaction.lsmTree.MemTable.ReplaceNewTable()
	newSSTable := createSSTableFromMemtable(oldMemtable)
	compaction.lsmTree.SSTables[newSSTable.Header.Level] = append(compaction.lsmTree.SSTables[newSSTable.Header.Level], newSSTable)
}

func (compaction *Compaction) tryCompactionProcess() {
	config := configs.GetStorageEngineConfig()

	for level := config.LSMTreeConfig.FirstLevel; level >= 0; level-- {
		if len(compaction.lsmTree.SSTables[level]) <= 1<<level {
			break
		}

		currentLevel := uint8(level)

		if level > 0 {
			compaction.mergeAndReplaceSSTables(currentLevel, currentLevel-1)
		} else {
			compaction.mergeAndReplaceSSTables(currentLevel, currentLevel)
		}
	}
}

func (compaction *Compaction) mergeAndReplaceSSTables(currentLevel, newLevel uint8) {
	mergedSSTable := mergeSSTables(compaction.lsmTree.SSTables[currentLevel], newLevel)
	compaction.lsmTree.SSTables[currentLevel] = make([]*sstable.SSTable, 0)
	compaction.lsmTree.SSTables[newLevel] = append(compaction.lsmTree.SSTables[newLevel], mergedSSTable)
}

func mergeSSTables(sstables []*sstable.SSTable, newLevel uint8) *sstable.SSTable {
	keyValues := make(map[string]*sstable.SSTableEntry)

	// Deduplication
	for _, ssTable := range sstables {
		for _, block := range ssTable.Blocks {
			for _, entry := range block.Entries {
				keyValues[entry.Key] = entry
			}
		}
	}

	// List entries
	var mergedSSTableEntries []*sstable.SSTableEntry
	for _, sstableEntries := range keyValues {
		mergedSSTableEntries = append(mergedSSTableEntries, sstableEntries)
	}

	sort.Slice(mergedSSTableEntries, func(i, j int) bool {
		return mergedSSTableEntries[i].Key < mergedSSTableEntries[j].Key
	})

	return sstable.CreateSSTable(mergedSSTableEntries, newLevel)
}

func createSSTableFromMemtable(memTable *memtable.FlushableMemTable) *sstable.SSTable {
	sstableEntries := make([]*sstable.SSTableEntry, 0)
	config := configs.GetStorageEngineConfig()

	for k, v := range memTable.Table {
		sstableEntries = append(sstableEntries, sstable.NewSSTableEntry(k, v.Value, v.IsTombstone))
	}

	return sstable.CreateSSTable(sstableEntries, uint8(config.LSMTreeConfig.FirstLevel))
}
