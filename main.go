package main

import (
	"fmt"
	"pkvstore/internal/storageengine/lsmtree"
	"strconv"
)

func main() {
	lsm := lsmtree.NewLSMTree()

	for i := int64(0); i < 130; i++ {
		lsm.Put(strconv.FormatInt(i, 10), strconv.FormatInt(i, 10))
	}

	// // Get values
	fmt.Println(lsm.Get("1")) // Output: one
	fmt.Println(lsm.Get("2")) // Output: two
	lsm.Delete("3")
	fmt.Println(lsm.Get("3")) // Output: three
	fmt.Println(lsm.Get("4")) // Output: <empty string>, false

	// sst := sstable.NewSSTable(1)
	// sst.AddEntry("1", "one", false)
	// sst.AddEntry("2", "two", false)
	// sst.AddEntry("1", "three", false)

	// err := sst.WriteToFile(strconv.FormatInt(int64(int(sst.Header.Level)), 10) + "-" + strconv.FormatInt(sst.Header.Timestamp, 10))

	// fmt.Println(err)
}
