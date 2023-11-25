package models

type ResultStatus int

const (
	Found ResultStatus = iota + 1
	NotFound
	Deleted
	ContinueSearch
)

type Result struct {
	Status ResultStatus
	Value  string
}

func NewFoundResult(value string) *Result {
	return &Result{
		Status: Found,
		Value:  value,
	}
}

func NewNotFoundResult() *Result {
	return &Result{
		Status: NotFound,
	}
}

func NewDeletedResult() *Result {
	return &Result{
		Status: Deleted,
	}
}

func NewContinueSearchResult() *Result {
	return &Result{
		Status: ContinueSearch,
	}
}
