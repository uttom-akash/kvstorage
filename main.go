package main

import (
	"fmt"
	"log"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"pkvstore/internal/storageengine/store"
	"strconv"
	"sync"
	"time"
)

func generateAndPutData(current int, lsm *store.Store, wg *sync.WaitGroup) {
	defer wg.Done()

	var key string

	for i := int64(0); i < 10000; i++ {
		randomNumber := rand.Int63n(1<<63 - 1)
		key = strconv.FormatInt(randomNumber, 10)
		value := strconv.FormatInt(randomNumber, 10)

		if i%2 == 0 {
			lsm.Put(key, value)
		} else {
			lsm.Get(key)
		}

	}
	fmt.Printf("Goroutine %d completed\n", current)
}

func main() {
	// Start profiling server
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	store := store.NewStore()
	store.Put("name", "akash")

	start := time.Now()

	numGoroutines := 1000
	var wg sync.WaitGroup
	wg.Add(numGoroutines)
	for j := 0; j < numGoroutines; j++ {
		go generateAndPutData(j, store, &wg)
	}

	wg.Wait()

	fmt.Println("name: ", store.Get("name")) // Output: one

	end := time.Now()
	duration := end.Sub(start)
	fmt.Println("duration: ", duration.Minutes())

	time.Sleep(time.Hour)
}
