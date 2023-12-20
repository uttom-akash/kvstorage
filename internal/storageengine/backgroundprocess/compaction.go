package backgroundprocess

import (
	"container/heap"
	"pkvstore/internal/core"
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
	compaction := &Compaction{
		lsmTree:    lsmtree,
		sharedChan: channels.GetSharedChannel(),
	}

	go compaction.listenFlushMemtable()
	go compaction.listenToCompact()

	return compaction
}

func (compaction *Compaction) listenToCompact() {

	for event := range compaction.sharedChan.CompactionEvent {

		if event < 1 {
			continue
		}

		compaction.tryCompactionProcess()
	}
}

func (compaction *Compaction) listenFlushMemtable() {

	for event := range compaction.sharedChan.FlushMemtableEvent {

		if event < 1 || compaction.lsmTree.MemTable.ReadOnlyTable == nil {
			continue
		}

		newSSTable := createSSTableFromMemtable(compaction.lsmTree.MemTable.ReadOnlyTable)
		compaction.lsmTree.SSTables[newSSTable.Header.Level] = append(compaction.lsmTree.SSTables[newSSTable.Header.Level], newSSTable)
		compaction.lsmTree.MemTable.ClearReadOnlyMemtable()

		compaction.sharedChan.CompactionEvent <- 1
	}
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
	mergedSSTable := mergeGetSSTables(compaction.lsmTree.SSTables[currentLevel], newLevel)
	compaction.lsmTree.SSTables[currentLevel] = make([]*sstable.SSTable, 0)
	compaction.lsmTree.SSTables[newLevel] = append(compaction.lsmTree.SSTables[newLevel], mergedSSTable)
}

func createSSTableFromMemtable(memTable map[string]*memtable.MemTableEntry) *sstable.SSTable {
	sstableEntries := make([]*sstable.SSTableEntry, 0)
	config := configs.GetStorageEngineConfig()

	for k, v := range memTable {
		sstableEntries = append(sstableEntries, sstable.NewSSTableEntry(k, v.Value, v.IsTombstone))
	}

	sort.Slice(sstableEntries, func(i, j int) bool {
		return sstableEntries[i].Key < sstableEntries[j].Key
	})

	return sstable.CreateSSTable(sstableEntries, uint8(config.LSMTreeConfig.FirstLevel))
}

func mergeGetSSTables(sstablesInLevel []*sstable.SSTable, newLevel uint8) *sstable.SSTable {
	frontier := make(core.PriorityQueue, 0)
	numberEntries := uint(0)

	for sstableID, ssTable := range sstablesInLevel {
		numberEntries += ssTable.Header.NumberEntries

		firstBlock := ssTable.Blocks[0]
		heap.Push(&frontier, &core.Item{
			SortKey:   firstBlock.Entries[0].Key,
			SSTableID: sstableID,
			BlockID:   0,
			EntryID:   0,
		})
	}

	newSSTable := sstable.OpenSSTable(newLevel, numberEntries)
	lastKey := ""

	for len(frontier) > 0 {
		item := heap.Pop(&frontier).(*core.Item)

		// Deduplication
		entry := sstablesInLevel[item.SSTableID].Blocks[item.BlockID].Entries[item.EntryID]

		if lastKey != item.SortKey {
			newSSTable.AddEntry(entry)
			lastKey = entry.Key
		}

		if item.EntryID+1 < len(sstablesInLevel[item.SSTableID].Blocks[item.BlockID].Entries) {
			block := sstablesInLevel[item.SSTableID].Blocks[item.BlockID]
			heap.Push(&frontier, &core.Item{
				SortKey:   block.Entries[item.EntryID+1].Key,
				SSTableID: item.SSTableID,
				BlockID:   item.BlockID,
				EntryID:   item.EntryID + 1,
			})

			continue
		}

		if item.BlockID+1 < len(sstablesInLevel[item.SSTableID].Blocks) {
			block := sstablesInLevel[item.SSTableID].Blocks[item.BlockID+1]
			heap.Push(&frontier, &core.Item{
				SortKey:   block.Entries[0].Key,
				SSTableID: item.SSTableID,
				BlockID:   item.BlockID + 1,
				EntryID:   0,
			})
		}
	}

	return newSSTable.CompleteSSTableCreation()
}
