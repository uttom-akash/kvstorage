package storageservice

import (
	"pkvstore/internal/storageengine/store"
	"pkvstore/pkg/models"
)

type StorageService struct {
	store *store.Store
}

func NewStorageService() *StorageService {
	return &StorageService{
		store: store.NewStore(),
	}
}

func (s *StorageService) Put(command models.PutCommand) error {

	s.store.Put(command.Key, command.Value)

	return nil
}

func (s *StorageService) Get(command models.GetCommand) (string, error) {

	result := s.store.Get(command.Key)

	return result.Value, nil
}

func (s *StorageService) Delete(command models.DeleteCommand) error {

	s.store.Delete(command.Key)

	return nil
}
