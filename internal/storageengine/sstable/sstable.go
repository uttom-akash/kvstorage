package sstable

import (
	"fmt"
	"pkvstore/internal/models"
	"pkvstore/internal/storageengine/configs"
	"pkvstore/internal/storageengine/memtable"
	"strconv"
	"time"

	cuckoo "github.com/seiflotfy/cuckoofilter"
)

type SSTableHeader struct {
	Level     uint8
	Timestamp int64
	Version   string
	BlockSize uint32
}

type SSTableEntry struct {
	Key         string
	Value       string
	IsTombstone bool
}

type SSTableBlock struct {
	sequence     int
	Entries      []*SSTableEntry
	CuckooFilter *cuckoo.Filter
}

type SSTableFooter struct {
	Checksum uint32
}

type SSTable struct {
	Header       *SSTableHeader
	Blocks       []*SSTableBlock
	Footer       *SSTableFooter
	CuckooFilter *cuckoo.Filter
}

func NewSSTableHeader(level uint8, version string, blockSize uint32) *SSTableHeader {
	return &SSTableHeader{
		Level:     level,
		Timestamp: time.Now().Unix(),
		Version:   version,
		BlockSize: blockSize,
	}
}

func NewSSTableEntry(key string, value string, isTombstone bool) *SSTableEntry {
	return &SSTableEntry{
		Key:         key,
		Value:       value,
		IsTombstone: isTombstone,
	}
}

func NewSSTableBlock(sequence int) *SSTableBlock {
	return &SSTableBlock{
		sequence:     sequence,
		Entries:      make([]*SSTableEntry, 0),
		CuckooFilter: cuckoo.NewFilter(1000), // Adjust the capacity as needed
	}
}

func NewSSTable(level uint8) *SSTable {

	configs := configs.GetStorageEngineConfig()

	return &SSTable{
		Header:       NewSSTableHeader(level, configs.SSTableConfig.Version, uint32(configs.SSTableConfig.BlockCapacity)),
		Blocks:       make([]*SSTableBlock, 0),
		CuckooFilter: cuckoo.NewFilter(uint(configs.SSTableConfig.FilterCapacity)),
	}
}

func CreateSsTableFromMemtable(memTable *memtable.MemTable) *SSTable {

	configs := configs.GetStorageEngineConfig()

	sst := NewSSTable(uint8(configs.LSMTreeConfig.FirstLevel))

	for k, v := range memTable.Table {
		sst.AddEntry(k, v.Value, v.IsTombstone)
	}

	return sst
}

func (sstable *SSTable) AddEntry(key string, value string, isTobstone bool) {

	numberOfBlocks := len(sstable.Blocks)

	shouldCreateNewBlock := len(sstable.Blocks) == 0 || len(sstable.Blocks[len(sstable.Blocks)-1].Entries) >= int(sstable.Header.BlockSize)

	var lastBlock *SSTableBlock

	if shouldCreateNewBlock {

		sequence := numberOfBlocks + 1

		lastBlock = NewSSTableBlock(sequence)

		sstable.Blocks = append(sstable.Blocks, lastBlock)

	} else {

		lastBlock = sstable.Blocks[len(sstable.Blocks)-1]

	}

	newSSTableEntry := NewSSTableEntry(key, value, isTobstone)

	lastBlock.Entries = append(lastBlock.Entries, newSSTableEntry)

	lastBlock.CuckooFilter.InsertUnique([]byte(fmt.Sprint(key)))

	sstable.CuckooFilter.InsertUnique([]byte(fmt.Sprint(key)))
}

func (s *SSTable) DoesNotExist(key string) bool {
	return !s.CuckooFilter.Lookup([]byte(key))
}

func (s *SSTable) ReadFromSSTable(key string) *models.Result {
	// TODO: do binary search

	for _, block := range s.Blocks {
		for _, entry := range block.Entries {
			if entry.Key == key {

				if entry.IsTombstone {
					return models.NewDeletedResult()
				}

				return models.NewFoundResult(entry.Value)
			}
		}
	}

	return models.NewNotFoundResult()
}

func (sst *SSTable) GetFileName() string {
	return strconv.FormatInt(int64(int(sst.Header.Level)), 10) + "-" + strconv.FormatInt(sst.Header.Timestamp, 10)
}
