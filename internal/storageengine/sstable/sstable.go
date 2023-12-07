package sstable

import (
	"pkvstore/internal/core"
	"pkvstore/internal/models"
	"pkvstore/internal/storageengine/configs"
	"strconv"
	"time"
)

// SSTableHeader represents the header of an SSTable.
type SSTableHeader struct {
	Level         uint8
	Timestamp     int64
	Version       string
	BlockSize     uint32
	NumberEntries uint
	sealed        bool
}

// SSTableEntry represents an entry in an SSTable.
type SSTableEntry struct {
	Key         string
	Value       string
	IsTombstone bool
}

// SSTableBlock represents a block in an SSTable.
type SSTableBlock struct {
	Sequence int
	Anchor   *SSTableEntry
	Entries  []*SSTableEntry
	Filter   *core.BloomFilter
}

// SSTableFooter represents the footer of an SSTable.
type SSTableFooter struct {
	Checksum uint32
}

// SSTable represents a sorted string table.
type SSTable struct {
	Header *SSTableHeader
	Blocks []*SSTableBlock
	Footer *SSTableFooter
	Filter *core.BloomFilter
}

// newSSTableHeader creates a new SSTableHeader.
func newSSTableHeader(level uint8, version string, blockSize uint32, numberEntries uint) *SSTableHeader {
	return &SSTableHeader{
		Level:         level,
		Timestamp:     time.Now().Unix(),
		Version:       version,
		BlockSize:     blockSize,
		NumberEntries: numberEntries,
		sealed:        false,
	}
}

// NewSSTableEntry creates a new SSTableEntry.
func NewSSTableEntry(key string, value string, isTombstone bool) *SSTableEntry {
	return &SSTableEntry{
		Key:         key,
		Value:       value,
		IsTombstone: isTombstone,
	}
}

// newSSTableBlock creates a new SSTableBlock.
func newSSTableBlock(sequence int) *SSTableBlock {
	configs := configs.GetStorageEngineConfig()

	return &SSTableBlock{
		Sequence: sequence,
		Entries:  make([]*SSTableEntry, 0),
		Filter:   core.NewBloomFilter(uint(configs.SSTableConfig.BlockCapacity), configs.SSTableConfig.BlockFilterFalsePositive, "optimal"),
	}
}

func (sstblock *SSTableBlock) addEntry(entry *SSTableEntry) {
	if len(sstblock.Entries) == 0 {
		sstblock.Anchor = entry
	}
	sstblock.Entries = append(sstblock.Entries, entry)
}

// newSSTable creates a new SSTable.
func newSSTable(level uint8, numberOfEntries uint) *SSTable {
	configs := configs.GetStorageEngineConfig()

	return &SSTable{
		Header: newSSTableHeader(level, configs.SSTableConfig.Version, uint32(configs.SSTableConfig.BlockCapacity), numberOfEntries),
		Blocks: make([]*SSTableBlock, 0),
		Filter: core.NewBloomFilter(numberOfEntries, configs.SSTableConfig.FilterFalsePositive, "optimal"),
	}
}

// region
func OpenSSTable(level uint8, NumberEntries uint) *SSTable {
	configs := configs.GetStorageEngineConfig()
	return &SSTable{
		Header: newSSTableHeader(level, configs.SSTableConfig.Version, uint32(configs.SSTableConfig.BlockCapacity), 0),
		Blocks: make([]*SSTableBlock, 0),
		Filter: core.NewBloomFilter(NumberEntries/10, configs.SSTableConfig.FilterFalsePositive, "optimal"),
	}
}

func (sstable *SSTable) AddEntry(newSSTableEntry *SSTableEntry) {
	if sstable.Header.sealed {
		return
	}

	sstable.Header.NumberEntries += 1
	sstable.addEntry(newSSTableEntry)
}

func (sstable *SSTable) CompleteSSTableCreation() *SSTable {
	sstable.Header.sealed = true
	return sstable
}

//end region

// CreateSSTable creates an SSTable from SSTableEntries.
func CreateSSTable(sstableEntries []*SSTableEntry, level uint8) *SSTable {
	newSSTable := newSSTable(level, uint(len(sstableEntries)))

	for _, sstableEntry := range sstableEntries {
		newSSTable.addEntry(sstableEntry)
	}

	return newSSTable
}

// addEntry adds an entry to the SSTable.
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

// DoesNotExist checks if a key does not exist in the SSTable.
func (s *SSTable) DoesNotExist(key string) bool {
	return s.Filter.DoesNotExist([]byte(key))
}

// ReadFromSSTable reads a key from the SSTable.
func (s *SSTable) ReadFromSSTable(key string) *models.Result {

	lastSmallerOrEqualBlock := s.getLastSmallerBlock(key)

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

// similar to lower_bound implementation in c++
// lower_bound returns equal or greater than key
// this func return equal or smaller than key
func (s *SSTable) getLastSmallerBlock(key string) *SSTableBlock {

	low, high := 0, len(s.Blocks)-1

	var lastSmallerOrEqualBlock *SSTableBlock

	for low <= high {

		mid := (low + high) / 2

		block := s.Blocks[mid]

		if block.Anchor.Key <= key {

			lastSmallerOrEqualBlock = block

			low = mid + 1

		} else {

			high = mid - 1
		}
	}

	return lastSmallerOrEqualBlock
}

// GetFileName returns the file name of the SSTable.
func (sst *SSTable) GetFileName() string {
	return strconv.FormatInt(int64(int(sst.Header.Level)), 10) + "-" + strconv.FormatInt(sst.Header.Timestamp, 10)
}
