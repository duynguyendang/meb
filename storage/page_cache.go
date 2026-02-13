package storage

import (
	"container/list"
	"sync"
)

type PageCache struct {
	mu       sync.RWMutex
	pages    map[uint64]*list.Element
	lru      *list.List
	maxPages int
}

type cacheEntry struct {
	pageRef *PageRef
	key     uint64
}

func NewPageCache(maxPages int) *PageCache {
	if maxPages <= 0 {
		maxPages = 100
	}
	return &PageCache{
		pages:    make(map[uint64]*list.Element, maxPages),
		lru:      list.New(),
		maxPages: maxPages,
	}
}

func (c *PageCache) GetPage(pageID uint64) *PageRef {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	elem, ok := c.pages[pageID]
	c.mu.RUnlock()

	if ok {
		c.mu.Lock()
		c.lru.MoveToFront(elem)
		c.mu.Unlock()
		return elem.Value.(*cacheEntry).pageRef
	}

	return nil
}

func (c *PageCache) PutPage(pageID uint64, buffer *ZeroCopyBuffer) *PageRef {
	if c == nil || buffer == nil {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.pages[pageID]; ok {
		c.lru.MoveToFront(elem)
		entry := elem.Value.(*cacheEntry)
		entry.pageRef.Close()
		entry.pageRef = NewPageRef(buffer)
		return entry.pageRef
	}

	if c.lru.Len() >= c.maxPages {
		c.evictOldest()
	}

	pageRef := NewPageRef(buffer)
	entry := &cacheEntry{
		pageRef: pageRef,
		key:     pageID,
	}
	elem := c.lru.PushFront(entry)
	c.pages[pageID] = elem

	return pageRef
}

func (c *PageCache) evictOldest() {
	if c.lru.Len() == 0 {
		return
	}
	elem := c.lru.Back()
	if elem != nil {
		entry := elem.Value.(*cacheEntry)
		delete(c.pages, entry.key)
		entry.pageRef.Close()
		c.lru.Remove(elem)
	}
}

func (c *PageCache) RemovePage(pageID uint64) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	elem, ok := c.pages[pageID]
	if !ok {
		return
	}
	entry := elem.Value.(*cacheEntry)
	entry.pageRef.Close()
	delete(c.pages, pageID)
	c.lru.Remove(elem)
}

func (c *PageCache) Clear() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, elem := range c.pages {
		entry := elem.Value.(*cacheEntry)
		entry.pageRef.Close()
	}
	c.pages = make(map[uint64]*list.Element)
	c.lru = list.New()
}

func (c *PageCache) Len() int {
	if c == nil {
		return 0
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.lru.Len()
}

func (c *PageCache) MaxPages() int {
	if c == nil {
		return 0
	}
	return c.maxPages
}
