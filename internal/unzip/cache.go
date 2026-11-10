package unzip

import (
	"context"
	"errors"
	"io"
	"sync"
)

type cachedBlock struct {
	ready chan struct{}
	data  []byte
	err   error
	used  uint64
}

// blockCache shares immutable aligned ranges across ZIP headers and entry streams.
// Pending requests are coalesced; only completed blocks count toward LRU eviction.
type blockCache struct {
	mu       sync.Mutex
	blocks   map[int64]*cachedBlock
	clock    uint64
	capacity int
	size     int64
}

// readCached copies pinned archive ranges through a bounded shared block cache.
func (r *remoteArchive) readCached(ctx context.Context, p []byte, off int64) error {
	if off < 0 || off > r.size || int64(len(p)) > r.size-off {
		return io.ErrUnexpectedEOF
	}
	for len(p) > 0 {
		start := off / r.cache.size * r.cache.size
		block, err := r.loadBlock(ctx, start)
		if err != nil {
			return err
		}
		n := copy(p, block[off-start:])
		if n == 0 {
			return io.ErrUnexpectedEOF
		}
		p = p[n:]
		off += int64(n)
	}
	return ctx.Err()
}

// loadBlock lets one caller fetch a block while other callers await the same result.
// Failed loads are removed so subsequent attempts can recover; canceled waiters leave promptly.
func (r *remoteArchive) loadBlock(ctx context.Context, start int64) ([]byte, error) {
	cache := r.cache
	cache.mu.Lock()
	cache.clock++
	if block, ok := cache.blocks[start]; ok {
		block.used = cache.clock
		cache.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-block.ready:
			return block.data, block.err
		}
	}
	block := &cachedBlock{ready: make(chan struct{}), used: cache.clock}
	cache.blocks[start] = block
	cache.mu.Unlock()
	data := make([]byte, int(min(cache.size, r.size-start)))
	err := r.readRange(ctx, data, start)
	cache.mu.Lock()
	block.data, block.err = data, err
	if err != nil {
		delete(cache.blocks, start)
		block.data = nil
	}
	close(block.ready)
	cache.evictLocked()
	cache.mu.Unlock()
	return block.data, err
}

// evictLocked discards the least recently used completed buffers, retaining active fetches.
func (c *blockCache) evictLocked() {
	completed := 0
	for _, block := range c.blocks {
		select {
		case <-block.ready:
			completed++
		default:
		}
	}
	for completed > c.capacity {
		var oldest *cachedBlock
		var key int64
		for start, block := range c.blocks {
			select {
			case <-block.ready:
			default:
				continue
			}
			if oldest == nil || block.used < oldest.used {
				oldest = block
				key = start
			}
		}
		if oldest == nil {
			return
		}
		delete(c.blocks, key)
		completed--
	}
}

// validateEntryOffset resolves the local header lazily through the cache before reading payloads.
func validateEntryOffset(item entry, size int64) (int64, error) {
	offset, err := item.file.DataOffset()
	if err != nil {
		return 0, err
	}
	if offset < 0 || offset > size || item.file.CompressedSize64 > uint64(size-offset) {
		return 0, errors.New("ZIP 条目大小或偏移无效")
	}
	return offset, nil
}
