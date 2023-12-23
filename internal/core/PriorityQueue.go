package core

type Item struct {
	SortKey   string
	SSTableID int
	BlockID   int
	EntryID   int
	Index     int
}

type PriorityQueue []*Item

func (mpq PriorityQueue) Len() int {
	return len(mpq)
}

func (mpq PriorityQueue) Less(i, j int) bool {
	if mpq[i].SortKey == mpq[j].SortKey {
		return mpq[i].SSTableID > mpq[j].SSTableID
	}
	return mpq[i].SortKey < mpq[j].SortKey
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].Index = i
	pq[j].Index = j
}

func (pq *PriorityQueue) Push(x any) {
	n := len(*pq)
	item := x.(*Item)
	item.Index = n
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.Index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}
