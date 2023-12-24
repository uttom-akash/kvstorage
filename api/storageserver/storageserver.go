package storageserver

import (
	"log"
	"net"
	"net/rpc"
	"pkvstore/pkg/models"
	"pkvstore/pkg/storageservice"
)

type StorageServer struct {
	storageService *storageservice.StorageService
}

func NewStorageServer() {

	server := &StorageServer{
		storageService: storageservice.NewStorageService(),
	}

	rpc.Register(server)

	listener, err := net.Listen("tcp", ":1234")

	if err != nil {
		log.Fatal("Listen error:", err)
	}

	log.Println("Server listening on port 1234")

	rpc.Accept(listener)
}

func (s *StorageServer) Put(command models.PutCommand, reply *bool) error {

	err := s.storageService.Put(command)

	*reply = true

	return err
}

func (s *StorageServer) Get(command models.GetCommand, reply *string) error {

	result, err := s.storageService.Get(command)

	if err != nil {
		return err
	}

	*reply = result

	return nil
}

func (s *StorageServer) Delete(command models.DeleteCommand, reply *bool) error {
	err := s.storageService.Delete(command)

	if err == nil {
		*reply = true
		return nil
	}

	return err
}
