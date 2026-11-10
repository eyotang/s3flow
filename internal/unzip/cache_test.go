package unzip

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"golang.org/x/sync/errgroup"
)

// TestBlockCacheCoalescesAndEvicts checks concurrent readers share downloads and LRU memory is bounded.
func TestBlockCacheCoalescesAndEvicts(t *testing.T) {
	data := bytes.Repeat([]byte("abcdefgh"), 1024)
	client, opts := fixture(data)
	opts.partSize = 1024
	remote, err := openArchive(context.Background(), client, opts)
	if err != nil {
		t.Fatal(err)
	}
	group := new(errgroup.Group)
	for i := 0; i < 64; i++ {
		group.Go(func() error {
			p := make([]byte, 32)
			if _, err := remote.ReadAt(p, 20); err != nil {
				return err
			}
			if !bytes.Equal(p, data[20:52]) {
				return errors.New("cached data changed")
			}
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		t.Fatal(err)
	}
	if client.gets != 1 {
		t.Fatalf("shared block fetched %d times", client.gets)
	}
	for off := int64(1024); off < int64(len(data)); off += 1024 {
		if _, err := remote.ReadAt(make([]byte, 1), off); err != nil {
			t.Fatal(err)
		}
	}
	if len(remote.cache.blocks) > remote.cache.capacity {
		t.Fatal("cache grew beyond capacity")
	}
	if _, err := remote.ReadAt(make([]byte, 1), 0); err != nil {
		t.Fatal(err)
	}
	if client.gets != 9 {
		t.Fatalf("expected evicted block to reload, GETs=%d", client.gets)
	}
}

// TestCanceledCacheWaiterReturns checks waiting readers honor cancellation without disrupting the owner.
func TestCanceledCacheWaiterReturns(t *testing.T) {
	client, opts := fixture([]byte("data"))
	remote, err := openArchive(context.Background(), client, opts)
	if err != nil {
		t.Fatal(err)
	}
	remote.cache.blocks[0] = &cachedBlock{ready: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := remote.loadBlock(ctx, 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled waiter: %v", err)
	}
}
