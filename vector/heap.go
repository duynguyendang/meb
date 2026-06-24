package vector

type minHeap struct {
	items []SearchResult
	cap   int
}

func newMinHeap(capacity int) *minHeap {
	return &minHeap{
		items: make([]SearchResult, 0, capacity),
		cap:   capacity,
	}
}

func (h *minHeap) push(item SearchResult) {
	if len(h.items) < cap(h.items) {
		h.items = append(h.items, item)
		h.siftUp(len(h.items) - 1)
	} else if item.Score > h.items[0].Score {
		h.items[0] = item
		h.siftDown(0)
	}
}

func (h *minHeap) pop() SearchResult {
	if len(h.items) == 0 {
		return SearchResult{}
	}
	item := h.items[0]
	h.items[0] = h.items[len(h.items)-1]
	h.items = h.items[:len(h.items)-1]
	if len(h.items) > 0 {
		h.siftDown(0)
	}
	return item
}

func (h *minHeap) Len() int {
	return len(h.items)
}

func (h *minHeap) drain() []SearchResult {
	result := make([]SearchResult, len(h.items))
	for i := len(h.items) - 1; i >= 0; i-- {
		result[i] = h.pop()
	}
	return result
}

func (h *minHeap) siftUp(i int) {
	for i > 0 {
		parent := (i - 1) / 2
		if h.items[i].Score >= h.items[parent].Score {
			break
		}
		h.items[i], h.items[parent] = h.items[parent], h.items[i]
		i = parent
	}
}

func (h *minHeap) siftDown(i int) {
	n := len(h.items)
	for {
		smallest := i
		left := 2*i + 1
		right := 2*i + 2
		if left < n && h.items[left].Score < h.items[smallest].Score {
			smallest = left
		}
		if right < n && h.items[right].Score < h.items[smallest].Score {
			smallest = right
		}
		if smallest == i {
			break
		}
		h.items[i], h.items[smallest] = h.items[smallest], h.items[i]
		i = smallest
	}
}

type nodeDist struct {
	id   uint64
	dist float32
}

type minHeap2 struct {
	items []nodeDist
	cap   int
}

func newMinHeap2(capacity int) *minHeap2 {
	return &minHeap2{
		items: make([]nodeDist, 0, capacity),
		cap:   capacity,
	}
}

func (h *minHeap2) push(item nodeDist) {
	h.items = append(h.items, item)
	h.siftUp(len(h.items) - 1)
}

func (h *minHeap2) pop() nodeDist {
	if len(h.items) == 0 {
		return nodeDist{}
	}
	item := h.items[0]
	h.items[0] = h.items[len(h.items)-1]
	h.items = h.items[:len(h.items)-1]
	if len(h.items) > 0 {
		h.siftDown(0)
	}
	return item
}

func (h *minHeap2) Len() int {
	return len(h.items)
}

func (h *minHeap2) siftUp(i int) {
	for i > 0 {
		parent := (i - 1) / 2
		if h.items[i].dist >= h.items[parent].dist {
			break
		}
		h.items[i], h.items[parent] = h.items[parent], h.items[i]
		i = parent
	}
}

func (h *minHeap2) siftDown(i int) {
	n := len(h.items)
	for {
		smallest := i
		left := 2*i + 1
		right := 2*i + 2
		if left < n && h.items[left].dist < h.items[smallest].dist {
			smallest = left
		}
		if right < n && h.items[right].dist < h.items[smallest].dist {
			smallest = right
		}
		if smallest == i {
			break
		}
		h.items[i], h.items[smallest] = h.items[smallest], h.items[i]
		i = smallest
	}
}

type maxHeap struct {
	items []nodeDist
	cap   int
}

func newMaxHeap(capacity int) *maxHeap {
	return &maxHeap{
		items: make([]nodeDist, 0, capacity),
		cap:   capacity,
	}
}

func (h *maxHeap) push(item nodeDist) {
	h.items = append(h.items, item)
	h.siftUp(len(h.items) - 1)
}

func (h *maxHeap) pop() nodeDist {
	if len(h.items) == 0 {
		return nodeDist{}
	}
	item := h.items[0]
	h.items[0] = h.items[len(h.items)-1]
	h.items = h.items[:len(h.items)-1]
	if len(h.items) > 0 {
		h.siftDown(0)
	}
	return item
}

func (h *maxHeap) peek() nodeDist {
	if len(h.items) == 0 {
		return nodeDist{}
	}
	return h.items[0]
}

func (h *maxHeap) Len() int {
	return len(h.items)
}

func (h *maxHeap) drain() []nodeDist {
	result := make([]nodeDist, len(h.items))
	for i := len(h.items) - 1; i >= 0; i-- {
		result[i] = h.pop()
	}
	return result
}

func (h *maxHeap) siftUp(i int) {
	for i > 0 {
		parent := (i - 1) / 2
		if h.items[i].dist <= h.items[parent].dist {
			break
		}
		h.items[i], h.items[parent] = h.items[parent], h.items[i]
		i = parent
	}
}

func (h *maxHeap) siftDown(i int) {
	n := len(h.items)
	for {
		largest := i
		left := 2*i + 1
		right := 2*i + 2
		if left < n && h.items[left].dist > h.items[largest].dist {
			largest = left
		}
		if right < n && h.items[right].dist > h.items[largest].dist {
			largest = right
		}
		if largest == i {
			break
		}
		h.items[i], h.items[largest] = h.items[largest], h.items[i]
		i = largest
	}
}
