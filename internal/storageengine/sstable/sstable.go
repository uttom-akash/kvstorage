package sstable

import (
	"pkvstore/internal/core"
	"pkvstore/internal/models"
	"pkvstore/internal/storageengine/configs"
	"strconv"
	"time"
)

type SSTableHeader struct {
	Level         uint8
	Timestamp     int64
	Version       string
	BlockSize     uint32
	NumberEntries uint
}

type SSTableEntry struct {
	Key         string
	Value       string
	IsTombstone bool
}

type SSTableBlock struct {
	sequence int
	Anchor   *SSTableEntry
	Entries  []*SSTableEntry
	Filter   *core.BloomFilter
}

type SSTableFooter struct {
	Checksum uint32
}

type SSTable struct {
	Header *SSTableHeader
	Blocks []*SSTableBlock
	Footer *SSTableFooter
	Filter *core.BloomFilter
}

func newSSTableHeader(level uint8, version string, blockSize uint32) *SSTableHeader {
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

func newSSTableBlock(sequence int) *SSTableBlock {
	configs := configs.GetStorageEngineConfig()

	return &SSTableBlock{
		sequence: sequence,
		Entries:  make([]*SSTableEntry, 0),
		Filter:   core.NewBloomFilter(uint(configs.SSTableConfig.BlockCapacity), configs.SSTableConfig.BlockFilterFalsePositive, "optimal"), // Adjust the capacity as needed
		// bloom filter : https://hur.st/bloomfilter/?n=50000&p=0.00002&m=&k=
	}
}

func (sstblock *SSTableBlock) addEntry(entry *SSTableEntry) {
	if len(sstblock.Entries) == 0 {
		sstblock.Anchor = entry
	}
	sstblock.Entries = append(sstblock.Entries, entry)
}

func newSSTable(level uint8, numberOfEntries uint) *SSTable {

	configs := configs.GetStorageEngineConfig()

	return &SSTable{
		Header: newSSTableHeader(level, configs.SSTableConfig.Version, uint32(configs.SSTableConfig.BlockCapacity)),
		Blocks: make([]*SSTableBlock, 0),
		Filter: core.NewBloomFilter(numberOfEntries, configs.SSTableConfig.FilterFalsePositive, "optimal"),
	}
}

func CreateSSTable(sstableEntries []*SSTableEntry, level uint8) *SSTable {

	newSSTable := newSSTable(level, uint(len(sstableEntries)))

	for _, sstableEntry := range sstableEntries {
		newSSTable.addEntry(sstableEntry)
	}

	return newSSTable
}

func (sstable *SSTable) addEntry(newSSTableEntry *SSTableEntry) {

	numberOfBlocks := len(sstable.Blocks)

	shouldCreateNewBlock := len(sstable.Blocks) == 0 || len(sstable.Blocks[len(sstable.Blocks)-1].Entries) >= int(sstable.Header.BlockSize)

	var lastBlock *SSTableBlock

	if shouldCreateNewBlock {

		sequence := numberOfBlocks + 1

		lastBlock = newSSTableBlock(sequence)

		sstable.Blocks = append(sstable.Blocks, lastBlock)

	} else {

		lastBlock = sstable.Blocks[len(sstable.Blocks)-1]

	}

	lastBlock.addEntry(newSSTableEntry)

	lastBlock.Filter.Add([]byte(newSSTableEntry.Key))

	sstable.Filter.Add([]byte(newSSTableEntry.Key))
}

func (s *SSTable) DoesNotExist(key string) bool {
	return s.Filter.DoesNotExist([]byte(key))
}

func (s *SSTable) ReadFromSSTable(key string) *models.Result {

	var lastSmallerOrEqualBlock *SSTableBlock

	// TODO: do binary search
	for _, block := range s.Blocks {
		if block.Anchor.Key > key {
			break
		}

		lastSmallerOrEqualBlock = block
	}

	if lastSmallerOrEqualBlock == nil || lastSmallerOrEqualBlock.Filter.DoesNotExist([]byte(key)) {
		return models.NewNotFoundResult()
	}

	for _, entry := range lastSmallerOrEqualBlock.Entries {
		if entry.Key == key {

			if entry.IsTombstone {
				return models.NewDeletedResult()
			}

			return models.NewFoundResult(entry.Value)
		}
	}

	return models.NewNotFoundResult()
}

func (sst *SSTable) GetFileName() string {
	return strconv.FormatInt(int64(int(sst.Header.Level)), 10) + "-" + strconv.FormatInt(sst.Header.Timestamp, 10)
}
