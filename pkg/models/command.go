package models

type PutCommand struct {
	Key   string
	Value string
}

type GetCommand struct {
	Key string
}

type DeleteCommand struct {
	Key string
}
