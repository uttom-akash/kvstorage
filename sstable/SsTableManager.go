package sstable

import (
	"sort"
)

func MergeSSTables(sstables []*SSTable) *SSTable {

	keyValues := make(map[string]*SSTableEntry)

	for i := len(sstables) - 1; i >= 0; i-- {
		for _, kv := range sstables[i].Blocks[len(sstables[i].Blocks)-1].Entries {

			keyValues[kv.Key] = NewSSTableEntry(kv.Key, kv.Value, kv.IsTombstone)
		}
	}

	var mergedData []*SSTableEntry
	for _, value := range keyValues {
		mergedData = append(mergedData, value)
	}

	sort.Slice(mergedData, func(i, j int) bool {
		return mergedData[i].Key < mergedData[j].Key
	})

	sst := NewSSTable(sstables[0].Header.Level - 1)

	for _, entry := range mergedData {
		sst.AddEntry(entry.Key, entry.Value, entry.IsTombstone)
	}

	return sst
}
