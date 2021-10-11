package pager

import "golang.org/x/sys/unix"

type GclockCache struct {
	Pages []CacheEntry
	// Maps PageIdx -> CacheIdx
	PageIdxMapping map[int64]int
	// The index at which to continue the search
	// This is kept so earlier pages aren't replaaced more often than later ones.
	SearchIndex int
}

func NewGclockCache() *GclockCache {
	return &GclockCache{
		Pages:          make([]CacheEntry, 0, CACHE_SIZE),
		PageIdxMapping: make(map[int64]int),
		SearchIndex:    0,
	}
}

func (gc *GclockCache) Get(idx int64) *Page {
	entryCacheIdx, hit := gc.PageIdxMapping[idx]
	if hit {
		entry := &gc.Pages[entryCacheIdx]
		entry.ReferenceCounter++
		return entry.Page
	} else {
		return nil
	}
}

func (gc *GclockCache) findPageToReplace() (int, int64) {
	for {
		if len(gc.Pages) <= gc.SearchIndex {
			gc.SearchIndex = 0
		}
		gc.Pages[gc.SearchIndex].ReferenceCounter--
		if gc.Pages[gc.SearchIndex].ReferenceCounter <= 0 {
			replacementIdx := gc.SearchIndex

			// Increment once more so the newly cached page isn't the first candidate next time
			gc.SearchIndex++
			gc.SearchIndex %= CACHE_SIZE

			return replacementIdx, gc.Pages[replacementIdx].Page.Index
		}
		gc.SearchIndex++
		gc.SearchIndex %= CACHE_SIZE
	}
}

func (gc *GclockCache) Add(page *Page) int64 {
	if len(gc.PageIdxMapping) >= CACHE_SIZE {
		// Cache is full, uncache
		cacheIdx, pageIdx := gc.findPageToReplace()

		// Actually unmap the memory page
		unix.Munmap(gc.Pages[cacheIdx].Page.Memory)

		// Replace the cache entry
		gc.Pages[cacheIdx] = CacheEntry{
			Page:             page,
			ReferenceCounter: 1,
		}
		delete(gc.PageIdxMapping, pageIdx)
		gc.PageIdxMapping[page.Index] = cacheIdx
	} else {
		gc.Pages = append(gc.Pages, CacheEntry{
			Page:             page,
			ReferenceCounter: 1,
		})
		gc.PageIdxMapping[page.Index] = len(gc.Pages) - 1
	}

	return page.Index
}
