package main

import (
	"flag"
	"fmt"
	"os"
	"pkvstore/pkg/storageclient"
)

type CommandInterface struct {
	client *storageclient.StorageClient
}

func main() {

	cli := &CommandInterface{client: storageclient.NewStorageClient()}

	getCmd := flag.NewFlagSet("get", flag.ExitOnError)
	putCmd := flag.NewFlagSet("put", flag.ExitOnError)
	deleteCmd := flag.NewFlagSet("delete", flag.ExitOnError)

	if len(os.Args) < 2 {
		fmt.Println("expected 'get', 'put' or 'delete' subcommands")
		os.Exit(1)
	}

	switch os.Args[1] {
	case "get":
		cli.handleGet(getCmd)
	case "put":
		cli.handlePut(putCmd)
	case "delete":
		cli.handleDelete(deleteCmd)
	default:
		fmt.Println("expected 'get', 'put' or 'delete' subcommands")
		os.Exit(1)
	}
}

func (cli *CommandInterface) handleGet(getCmd *flag.FlagSet) {

	key := getCmd.String("key", "", "Key of the item")

	getCmd.Parse(os.Args[2:])

	value := cli.client.Get(*key)

	fmt.Println("GET operation - Key:", *key, " value: ", value)
}

func (cli *CommandInterface) handlePut(putCmd *flag.FlagSet) {

	key := putCmd.String("key", "", "Key of the item")

	value := putCmd.String("value", "", "Value of the item")

	putCmd.Parse(os.Args[2:])

	cli.client.Put(*key, *value)

	fmt.Println("PUT operation - Key:", *key, "Value:", *value)
}

func (cli *CommandInterface) handleDelete(deleteCmd *flag.FlagSet) {

	key := deleteCmd.String("key", "", "Key of the item")

	deleteCmd.Parse(os.Args[2:])

	cli.client.Delete(*key)

	fmt.Println("DELETE operation - Key:", *key)
}
