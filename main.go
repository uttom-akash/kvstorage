package main

import (
	"fmt"
	"math/rand"
	_ "net/http/pprof"
	"pkvstore/internal/storageengine/store"
	"strconv"
	"sync"
	"time"
)

func main() {
	// Start profiling server
	// go func() {
	// 	log.Println(http.ListenAndServe("localhost:6060", nil))
	// }()

	store := store.NewStore()

	store.Put("name1", "akash")

	store.Put("address1", "dhanmandi")

	store.Put("name2", "akash")

	store.Put("address2", "dhanmandi")

	store.Put("name3", "akash")

	store.Put("address3", "dhanmandi")

	store.Put("name", "ckash")

	fmt.Println("name: ", store.Get("name")) // Output: one

}

func generateAndPutData(current int, lsm *store.Store, wg *sync.WaitGroup) {
	defer wg.Done()

	var key string

	for i := int64(0); i < 10; i++ {
		randomNumber := rand.Int63n(1<<63 - 1)
		key = strconv.FormatInt(randomNumber, 10)
		value := strconv.FormatInt(randomNumber, 10)

		lsm.Put(key, value)
	}
	fmt.Printf("Goroutine %d completed\n", current)
}

func newUser(store *store.Store, i int) {
	start := time.Now()

	numGoroutines := 100
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for j := 0; j < numGoroutines; j++ {
		go generateAndPutData(j, store, &wg)
	}

	wg.Wait()

	end := time.Now()
	duration := end.Sub(start)
	fmt.Println("duration: ", i, duration.Minutes())
}
