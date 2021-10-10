package pager

import "testing"

func TestGclockCache(t *testing.T) {
	CACHE_SIZE = 2
	cache := NewGclockCache()

	outOfRangePage := cache.Get(0)
	if outOfRangePage != nil {
		t.Error("Got Page out of range")
	}

	page1 := &Page{Index: 0}
	page2 := &Page{Index: 1}
	page3 := &Page{Index: 2}

	cache.Add(page1)
	cache.Add(page2)

	if cache.Pages[0].Page != page1 {
		t.Error("First page not properly added to cache")
	}
	if cache.Pages[0].ReferenceCounter != 1 {
		t.Error("Initial reference count for first page not properly set")
	}

	page2ret := cache.Get(1)
	if page2ret != page2 {
		t.Error("Wrong page retrieved")
	}

	if cache.Pages[1].ReferenceCounter != 2 {
		t.Error("Refernce count not properly increased on access")
	}

	// Page 3 added, cache has length 2, page 1 should be replaced
	cache.Add(page3)

	if cache.Pages[0].Page != page3 {
		t.Error("Wrong page replaced")
	}

	if cache.Pages[0].ReferenceCounter != 1 {
		t.Error("Wrong reference count set on replacement")
	}
}
