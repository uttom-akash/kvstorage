package configs

import (
	"sync"
)

const NUMBER_LEVELS = 7 // sstables: first level = 2^6, last level = 2^0

type StorageEngineConfig struct {
	LSMTreeConfig struct {
		NumberOfSSTableLevels int
		FirstLevel            int
		LastLevel             int
	}

	SSTableConfig struct {
		Version                  string
		FirstLevel               int
		FilterFalsePositive      float64 //TODO: dynamic
		BlockCapacity            int     //fixed
		BlockFilterFalsePositive float64 //fixed
	}

	MemTableConfig struct {
		MaxCapacity int
	}
}

func NewStorageEngineConfig() *StorageEngineConfig {

	config := new(StorageEngineConfig)

	config.LSMTreeConfig.NumberOfSSTableLevels = NUMBER_LEVELS // sstables: first level = 2^6, last level = 2^0
	config.LSMTreeConfig.FirstLevel = config.LSMTreeConfig.NumberOfSSTableLevels - 1
	config.LSMTreeConfig.LastLevel = 0

	config.SSTableConfig.Version = "1.0.0"
	config.SSTableConfig.FirstLevel = config.LSMTreeConfig.FirstLevel
	config.SSTableConfig.FilterFalsePositive = 0.1        // 1 in 10, 500MB, hash function 3 for 10^9 keys
	config.SSTableConfig.BlockCapacity = 2048             //2048
	config.SSTableConfig.BlockFilterFalsePositive = 0.001 // 1 in 1000, 3.59KiB, hash function 10

	config.MemTableConfig.MaxCapacity = 16384 //4096

	return config
}

var lock = &sync.Mutex{}
var confighandlerInstance *StorageEngineConfig

func GetStorageEngineConfig() *StorageEngineConfig {

	if confighandlerInstance == nil {
		lock.Lock()
		defer lock.Unlock()

		if confighandlerInstance == nil {
			// fmt.Println("Creating single instance now.")
			confighandlerInstance = NewStorageEngineConfig()
		} else {
			// fmt.Println("Single instance already created.")
		}
	} else {
		// fmt.Println("Single instance already created.")
	}

	return confighandlerInstance
}
