package storageclient

import (
	"log"
	"net/rpc"
	"pkvstore/pkg/models"
)

type StorageClient struct {
	client *rpc.Client
}

func NewStorageClient() *StorageClient {
	client, err := rpc.Dial("tcp", "localhost:1234")
	if err != nil {
		log.Fatal("Dialing:", err)
	}

	return &StorageClient{
		client: client,
	}
}

func (s *StorageClient) Put(key string, value string) bool {

	putItem := models.PutCommand{Key: key, Value: value}

	var putReply bool

	err := s.client.Call("StorageServer.Put", putItem, &putReply)

	if err != nil {
		log.Fatal("StorageServer.Put error:", err)
	}

	return putReply
}

func (s *StorageClient) Get(key string) string {

	getItem := models.GetCommand{Key: key}

	var getReply string

	err := s.client.Call("StorageServer.Get", getItem, &getReply)

	if err != nil {
		log.Fatal("StorageServer.Get error:", err)
	}

	return getReply
}

func (s *StorageClient) Delete(key string) bool {

	deleteItem := models.DeleteCommand{Key: key}

	var deleteReply bool

	err := s.client.Call("StorageServer.Delete", deleteItem, &deleteReply)

	if err != nil {
		log.Fatal("StorageServer.Delete error:", err)
	}

	return deleteReply
}
