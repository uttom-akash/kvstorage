package channels

import "sync"

type SharedChannel struct {
	NewMutationEventChannel chan int
	SwitchMemtableEvent     chan int
	FlushMemtableEvent      chan int
	CompactionEvent         chan int
}

func newSharedChannel() *SharedChannel {
	return &SharedChannel{
		NewMutationEventChannel: make(chan int, 10000),
		SwitchMemtableEvent:     make(chan int, 10000),
		FlushMemtableEvent:      make(chan int, 100),
		CompactionEvent:         make(chan int, 10),
	}
}

var lock = &sync.Mutex{}
var confighandlerInstance *SharedChannel

func GetSharedChannel() *SharedChannel {

	if confighandlerInstance == nil {
		lock.Lock()
		defer lock.Unlock()

		if confighandlerInstance == nil {
			// fmt.Println("Creating single instance now.")
			confighandlerInstance = newSharedChannel()
		} else {
			// fmt.Println("Single instance already created.")
		}
	} else {
		// fmt.Println("Single instance already created.")
	}

	return confighandlerInstance
}
