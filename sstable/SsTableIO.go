package sstable

import (
	"encoding/binary"
	"fmt"
	"os"

	cuckoo "github.com/seiflotfy/cuckoofilter"
)

const SSTABLE_FOLDER = "/storage/sstable/"

func LoadFromFile(filename string) (*SSTable, error) {

	file, err := os.Open(SSTABLE_FOLDER + filename)

	if err != nil {
		return nil, err
	}

	defer file.Close()

	// Read header
	var header SSTableHeader
	if err := binary.Read(file, binary.LittleEndian, &header); err != nil {
		return nil, err
	}

	// Read blocks
	blocks := make([]*SSTableBlock, 0)
	tableFilter := cuckoo.NewFilter(1000)
	for {
		var blockSize uint32

		if err := binary.Read(file, binary.LittleEndian, &blockSize); err != nil {
			break // End of file reached
		}

		entries, shouldReturn, returnValue, returnValue1 := ReadBlock(blockSize, file)
		if shouldReturn {
			return returnValue, returnValue1
		}

		// Create a Cuckoo Filter and populate it
		cuckooFilter := cuckoo.NewFilter(1000) // Adjust the capacity as needed
		for _, entry := range entries {
			cuckooFilter.InsertUnique([]byte(fmt.Sprint(entry.Key)))
			tableFilter.InsertUnique([]byte(fmt.Sprint(entry.Key)))
		}

		blocks = append(blocks, &SSTableBlock{
			Entries:      entries,
			CuckooFilter: cuckooFilter,
		})
	}

	// Read footer
	var footer SSTableFooter
	if err := binary.Read(file, binary.LittleEndian, &footer); err != nil {
		return nil, err
	}

	return &SSTable{
		Header:       &header,
		Blocks:       blocks,
		Footer:       &footer,
		CuckooFilter: tableFilter,
	}, nil
}

func ReadBlock(blockSize uint32, file *os.File) ([]*SSTableEntry, bool, *SSTable, error) {
	entries := make([]*SSTableEntry, blockSize)
	for i := 0; i < int(blockSize); i++ {
		if err := binary.Read(file, binary.LittleEndian, &entries[i].Key); err != nil {
			return nil, true, nil, err
		}
		var valueSize uint32
		if err := binary.Read(file, binary.LittleEndian, &valueSize); err != nil {
			return nil, true, nil, err
		}
		valueBytes := make([]byte, valueSize)
		if _, err := file.Read(valueBytes); err != nil {
			return nil, true, nil, err
		}
		entries[i].Value = string(valueBytes)

		if err := binary.Read(file, binary.LittleEndian, &entries[i].IsTombstone); err != nil {
			return nil, true, nil, err
		}
	}
	return entries, false, nil, nil
}

func (sstable *SSTable) WriteToFile(filename string) error {

	file, err := os.Create("./" + SSTABLE_FOLDER + filename)

	if err != nil {
		return err
	}

	defer file.Close()

	// Write header
	wrerr := binary.Write(file, binary.LittleEndian, sstable.Header)

	if wrerr != nil {
		return wrerr
	}

	// Write blocks
	for _, block := range sstable.Blocks {

		wrerr := binary.Write(file, binary.LittleEndian, uint32(len(block.Entries)))

		if wrerr != nil {
			return wrerr
		}

		shouldReturn, returnValue := WriteBlock(block, file)

		if shouldReturn {
			return returnValue
		}
	}

	// if err := binary.Write(file, binary.LittleEndian, sstable.Footer); err != nil {
	// 	return err
	// }

	return nil
}

func WriteBlock(block *SSTableBlock, file *os.File) (bool, error) {

	for _, entry := range block.Entries {

		if err := binary.Write(file, binary.LittleEndian, entry.Key); err != nil {
			return true, err
		}

		valueBytes := []byte(entry.Value)
		if err := binary.Write(file, binary.LittleEndian, uint32(len(valueBytes))); err != nil {
			return true, err
		}

		if _, err := file.Write(valueBytes); err != nil {
			return true, err
		}

		if err := binary.Write(file, binary.LittleEndian, entry.IsTombstone); err != nil {
			return true, err
		}
	}

	return false, nil
}
