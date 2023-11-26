package configs

import (
	"fmt"
	"sync"
)

const NUMBER_LEVELS = 7 // sstables: first level = 2^6, last level = 2^0

type StorageEngineConfig struct {
	LSMTreeConfig struct {
		SSTableLevels int
		FirstLevel    int
		LastLevel     int
	}

	SSTableConfig struct {
		Version             string
		FirstLevel          int
		FilterCapacity      int //TODO: dynamic
		BlockCapacity       int //fixed
		BlockFilterCapacity int //fixed
	}

	MemTableConfig struct {
		MaxCapacity int
	}
}

func NewStorageEngineConfig() *StorageEngineConfig {

	config := new(StorageEngineConfig)

	config.LSMTreeConfig.SSTableLevels = NUMBER_LEVELS // sstables: first level = 2^6, last level = 2^0
	config.LSMTreeConfig.FirstLevel = NUMBER_LEVELS - 1
	config.LSMTreeConfig.LastLevel = 0

	config.SSTableConfig.Version = "1.0.0"
	config.SSTableConfig.FirstLevel = NUMBER_LEVELS - 1
	config.SSTableConfig.FilterCapacity = 1000
	config.SSTableConfig.BlockCapacity = 10 //256
	config.SSTableConfig.BlockFilterCapacity = 1000

	config.MemTableConfig.MaxCapacity = 3 //4096

	return config
}

var lock = &sync.Mutex{}
var confighandlerInstance *StorageEngineConfig

func GetStorageEngineConfig() *StorageEngineConfig {

	if confighandlerInstance == nil {
		lock.Lock()
		defer lock.Unlock()

		if confighandlerInstance == nil {
			fmt.Println("Creating single instance now.")
			confighandlerInstance = NewStorageEngineConfig()
		} else {
			fmt.Println("Single instance already created.")
		}
	} else {
		fmt.Println("Single instance already created.")
	}

	return confighandlerInstance
}
