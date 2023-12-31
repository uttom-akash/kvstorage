package main

import (
	_ "net/http/pprof"
	"pkvstore/api/storageserver"
)

// func main() {
// 	// Start profiling server
// 	// go func() {
// 	// 	log.Println(http.ListenAndServe("localhost:6060", nil))
// 	// }()

// 	store := store.NewStore()

// 	store.Put("name1", "akash")

// 	store.Put("address1", "dhanmandi")

// 	store.Put("name2", "bkash")

// 	store.Put("address2", "dhanmandi")

// 	store.Put("name3", "dkash")

// 	store.Put("address3", "dhanmandi")

// 	store.Put("name", "ckash")

// 	store.Delete("name")

// 	newUser(store, 10)

// 	fmt.Println("name1: ", store.Get("name1")) // Output: one
// 	fmt.Println("name2: ", store.Get("name2")) // Output: one
// 	fmt.Println("name3: ", store.Get("name3")) // Output: one
// 	fmt.Println("name: ", store.Get("name"))   // Output: one

// }

// func generateAndPutData(current int, lsm *store.Store, wg *sync.WaitGroup) {
// 	defer wg.Done()

// 	var key string

// 	for i := int64(0); i < 100; i++ {
// 		randomNumber := rand.Int63n(1<<63 - 1)
// 		key = strconv.FormatInt(randomNumber, 10)
// 		value := strconv.FormatInt(randomNumber, 10)

// 		lsm.Put(key, value)

// 		// fmt.Println("get:", lsm.Get(key))
// 	}

// 	fmt.Printf("Goroutine %d completed\n", current)
// }

// func newUser(store *store.Store, i int) {
// 	start := time.Now()

// 	numGoroutines := 100
// 	var wg sync.WaitGroup
// 	wg.Add(numGoroutines)

// 	for j := 0; j < numGoroutines; j++ {
// 		go generateAndPutData(j, store, &wg)

// 		time.Sleep(3 * time.Second)
// 	}

// 	wg.Wait()

// 	end := time.Now()
// 	duration := end.Sub(start)
// 	fmt.Println("duration: ", i, duration.Minutes())
// }

func main() {

	storageserver.NewStorageServer()

	// time.Sleep(time.Second * 10)

	// client := storageclient.NewStorageClient()

	// client.Put("name1", "akash")

	// client.Put("address1", "dhanmandi")

	// client.Put("name2", "bkash")

	// client.Put("address2", "dhanmandi")

	// client.Put("name3", "dkash")

	// client.Put("address3", "dhanmandi")

	// client.Put("name", "ckash")

	// fmt.Println(client.Get("name1"))

	// fmt.Println(client.Delete("name1"))

	// fmt.Println(client.Get("name1"))
}
