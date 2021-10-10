package pager

type CacheEntry struct {
	Page             *Page
	ReferenceCounter int64
}

type Cache interface {
	// Request a page from cache, nil if there was no hit
	Get(int64) *Page
	Add(*Page) int64
}

// The maximum number of pages kept in cache
var CACHE_SIZE = 32
