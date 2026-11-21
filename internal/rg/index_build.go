package rg

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/eyotang/s3flow/internal/pkg"
)

type indexBuildClient interface {
	searchClient
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
}

type indexBuilder struct {
	client indexBuildClient
	reader indexReader
	opts   options
	stderr io.Writer
}

type listedEntry struct {
	objectMeta
	relative string
	decoding uint8
}

func buildIndexes(ctx context.Context, client indexBuildClient, opts options, stderr io.Writer) error {
	b := indexBuilder{client: client, reader: indexReader{client: client}, opts: opts, stderr: stderr}
	failed := false
	for _, source := range opts.targets {
		place := indexPlace(source)
		if opts.indexAt != nil {
			place = indexPlace(*opts.indexAt)
		}
		clean, err := b.build(ctx, source, place)
		if err != nil {
			fmt.Fprintf(stderr, "s3rg: %s: %v\n", source.uri(), err)
		}
		failed = failed || !clean || err != nil
		if ctx.Err() != nil {
			break
		}
	}
	if failed || ctx.Err() != nil {
		return pkg.ExitError(2)
	}
	return nil
}

func (b *indexBuilder) build(ctx context.Context, prefix, place target) (bool, error) {
	if prefix.key != "" && !strings.HasSuffix(prefix.key, "/") {
		_, err := b.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(prefix.bucket), Key: aws.String(prefix.key)})
		if err == nil {
			return false, errors.New("an index is built for a prefix; use a trailing '/' to select it")
		}
		if !s3Missing(err) && !s3Status(err, 403) {
			return false, err
		}
		prefix.key += "/"
	}
	source := indexSource{Root: "s3://" + prefix.bucket, Prefix: strings.TrimSuffix(prefix.key, "/")}
	for range 5 {
		root, revision, err := b.previous(ctx, place, source)
		if err != nil {
			return false, err
		}
		var entries []listedEntry
		found, err := walkSearchObjects(ctx, b.client, prefix, nil, func(object objectMeta, relative string) error {
			entries = append(entries, listedEntry{object, relative, objectDecoding(object.key, b.opts.noDecompress)})
			return nil
		})
		if err != nil {
			return false, err
		}
		if !found && prefix.key != "" && revision == "" {
			return false, errors.New("S3 prefix contains no objects")
		}
		slices.SortFunc(entries, func(a, b listedEntry) int { return strings.Compare(a.relative, b.relative) })
		entries = slices.CompactFunc(entries, func(a, b listedEntry) bool { return a.relative == b.relative })
		clean, err := b.publish(ctx, place, root, revision, entries)
		if !s3Status(err, 412) && !s3Status(err, 409) {
			return clean, err
		}
	}
	return false, errors.New("other builds replaced the index root five times; run the build again")
}

func (b *indexBuilder) previous(ctx context.Context, place target, source indexSource) (indexRoot, string, error) {
	empty := indexRoot{Format: indexVersion, Source: source, Decoders: map[string]uint32{"gzip": 1}, Segments: []segmentRef{}}
	data, revision, err := b.reader.root(ctx, place)
	if s3Missing(err) {
		return empty, "", nil
	}
	if err != nil {
		return empty, "", err
	}
	if revision == "" {
		return empty, "", errors.New("the index root has no ETag for a conditional update")
	}
	previous, err := parseIndexRoot(data)
	if previous.Source.Root != "" && previous.Source != source {
		return empty, "", fmt.Errorf("index belongs to %s/%s", previous.Source.Root, previous.Source.Prefix)
	}
	if err == nil {
		return previous, revision, nil
	}
	if b.opts.indexRebuild {
		return empty, revision, nil
	}
	return empty, "", fmt.Errorf("%w; use --index-rebuild to replace it", err)
}

type changedSegment struct {
	position int
	dead     []uint32
	allDead  bool
}

func (b *indexBuilder) changes(ctx context.Context, place target, root indexRoot, entries []listedEntry) ([]listedEntry, []changedSegment, error) {
	if b.opts.indexRebuild {
		changes := make([]changedSegment, 0, len(root.Segments))
		for i := range root.Segments {
			changes = append(changes, changedSegment{position: i, allDead: true})
		}
		return entries, changes, nil
	}
	positions := make(map[string]int, len(entries))
	for i, entry := range entries {
		positions[entry.relative] = i
	}
	seen, covered := make([]bool, len(entries)), make([]bool, len(entries))
	var changes []changedSegment
	for i, ref := range slices.Backward(root.Segments) {
		table, err := b.reader.table(ctx, place, ref)
		if err != nil {
			return nil, nil, fmt.Errorf("%w; use --index-rebuild to replace it", err)
		}
		var newlyDead []uint32
		for id, old := range table.entries {
			if _, dead := slices.BinarySearch(table.dead, uint32(id)); dead {
				continue
			}
			position, listed := positions[old.key]
			if listed && !seen[position] {
				seen[position] = true
				entry := entries[position]
				if old.ok && root.decoderMatches(entry.decoding) && old.describes(entry.objectMeta, entry.decoding) {
					covered[position] = true
					continue
				}
			}
			newlyDead = append(newlyDead, uint32(id))
		}
		if len(newlyDead) != 0 {
			dead := append(table.dead, newlyDead...)
			slices.Sort(dead)
			changes = append(changes, changedSegment{i, dead, len(dead) == len(table.entries)})
		}
	}
	var needed []listedEntry
	for i, entry := range entries {
		if !covered[i] {
			needed = append(needed, entry)
		}
	}
	return needed, changes, nil
}

func (b *indexBuilder) publish(ctx context.Context, place target, root indexRoot, revision string, entries []listedEntry) (bool, error) {
	needed, changes, err := b.changes(ctx, place, root, entries)
	if err != nil {
		return false, err
	}
	if revision != "" && !b.opts.indexRebuild && len(needed) == 0 && len(changes) == 0 {
		if b.opts.indexStats {
			fmt.Fprintf(b.stderr, "s3rg: index %s: %d objects indexed, 0 objects read (up to date, nothing written)\n", place.uri(), len(entries))
		}
		return true, nil
	}
	if err := b.put(ctx, place, indexMarker, []byte("This directory is a search index written by s3rg.\n"), "", true); err != nil && !s3Status(err, 412) {
		return false, err
	}
	now := uint64(time.Now().Unix())
	for _, change := range changes {
		ref := &root.Segments[change.position]
		if change.allDead {
			for _, key := range ref.keys() {
				root.Retired = append(root.Retired, retiredBlob{key, now})
			}
			ref.ID = ""
			continue
		}
		if ref.Dead != nil {
			root.Retired = append(root.Retired, retiredBlob{ref.Dead.Key, now})
		}
		data := appendIndexIDs([]byte(deadMagic), change.dead)
		dead, err := b.writeChecked(ctx, place, "seg/"+rand.Text()+".dead", data)
		if err != nil {
			return false, err
		}
		ref.Dead = &dead
	}
	root.Segments = slices.DeleteFunc(root.Segments, func(ref segmentRef) bool { return ref.ID == "" })
	segments, failed, err := b.writeEntries(ctx, place, needed, now)
	if err != nil {
		return false, err
	}
	root.Segments = append(root.Segments, segments...)
	root.Decoders["gzip"] = 1
	root.Created = now
	root.Generation++
	data, err := json.Marshal(root)
	if err != nil {
		return false, err
	}
	if len(data)+1 > maxIndexRootBytes {
		return false, errors.New("index root is too large; use --index-rebuild to consolidate it")
	}
	if err := b.put(ctx, place, indexRootName, append(data, '\n'), revision, revision == ""); err != nil {
		return false, err
	}
	if b.opts.indexStats {
		fmt.Fprintf(b.stderr, "s3rg: index %s: %d objects indexed, %d not covered, %d objects read, %d segments\n",
			place.uri(), len(entries)-failed, failed, len(needed), len(root.Segments))
	}
	return failed == 0, nil
}

func (b *indexBuilder) put(ctx context.Context, place target, name string, data []byte, revision string, create bool) error {
	input := &s3.PutObjectInput{
		Bucket: aws.String(place.bucket), Key: aws.String(place.key + name),
		Body: bytes.NewReader(data), ContentLength: aws.Int64(int64(len(data))),
	}
	if create {
		input.IfNoneMatch = aws.String("*")
	} else if revision != "" {
		input.IfMatch = aws.String(revision)
	}
	_, err := b.client.PutObject(ctx, input)
	if err != nil {
		return fmt.Errorf("write %s: %w", name, err)
	}
	return nil
}

type gramRead struct {
	grams []uint32
	err   error
}

const maxSegmentPairs = 4 << 20

func (b *indexBuilder) writeEntries(ctx context.Context, place target, entries []listedEntry, now uint64) ([]segmentRef, int, error) {
	var refs []segmentRef
	var records []indexEntry
	var pairs []uint64
	failed := 0
	flush := func() error {
		if len(records) == 0 {
			return nil
		}
		ref, err := b.writeSegment(ctx, place, records, pairs, now)
		if err != nil {
			return err
		}
		refs = append(refs, ref)
		records, pairs = records[:0], pairs[:0]
		return nil
	}
	// Bound memory for the completed gram sets while preserving key order.
	for batch := range slices.Chunk(entries, min(b.opts.jobs, 8)) {
		results := make([]gramRead, len(batch))
		var workers sync.WaitGroup
		for i, entry := range batch {
			workers.Go(func() {
				results[i].grams, results[i].err = b.readGrams(ctx, entry)
			})
		}
		workers.Wait()
		if ctx.Err() != nil {
			return nil, failed, ctx.Err()
		}
		for i, entry := range batch {
			result := results[i]
			if result.err != nil {
				failed++
				fmt.Fprintf(b.stderr, "s3rg: %s: %v\n", entry.uri(), result.err)
			}
			if len(pairs)+len(result.grams) > maxSegmentPairs || len(records) >= 65536 {
				if err := flush(); err != nil {
					return nil, failed, err
				}
			}
			id := uint64(len(records))
			size := max(0, aws.ToInt64(entry.size))
			records = append(records, indexEntry{entry.relative, uint64(size), aws.ToString(entry.etag), entry.decoding, result.err == nil})
			for _, gram := range result.grams {
				pairs = append(pairs, uint64(gram)<<24|id)
			}
		}
	}
	err := flush()
	return refs, failed, err
}

func (b *indexBuilder) readGrams(ctx context.Context, entry listedEntry) ([]uint32, error) {
	if !entry.hasRevision() {
		return nil, errors.New("the listing has no size or ETag for this object")
	}
	result, err := b.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(entry.bucket), Key: aws.String(entry.key), IfMatch: entry.etag,
	}, disableMissingChecksumWarning)
	if err != nil {
		return nil, err
	}
	defer result.Body.Close()
	if aws.ToString(result.ETag) != *entry.etag || (result.ContentLength != nil && *result.ContentLength != *entry.size) {
		return nil, errors.New("object changed after listing")
	}
	reader, finish, err := decodeContent(result.Body, entry.decoding)
	if err != nil {
		return nil, err
	}
	defer finish()
	return collectGrams(reader)
}

var gramBitmaps = sync.Pool{New: func() any { return new([maxIndexIDs / 64]uint64) }}

func collectGrams(reader io.Reader) ([]uint32, error) {
	bitmap := gramBitmaps.Get().(*[maxIndexIDs / 64]uint64)
	var grams []uint32
	defer func() {
		for _, gram := range grams {
			bitmap[gram/64] = 0
		}
		gramBitmaps.Put(bitmap)
	}()
	var buffer [64 << 10]byte
	var gram uint32
	seen := 0
	for {
		n, err := reader.Read(buffer[:])
		for _, c := range buffer[:n] {
			gram = (gram<<8 | uint32(c)) & (maxIndexIDs - 1)
			if seen < 2 {
				seen++
				continue
			}
			word, bit := gram/64, uint64(1)<<(gram%64)
			if bitmap[word]&bit == 0 {
				bitmap[word] |= bit
				grams = append(grams, gram)
			}
		}
		if errors.Is(err, io.EOF) {
			return grams, nil
		}
		if err != nil {
			return nil, err
		}
	}
}

func (b *indexBuilder) writeChecked(ctx context.Context, place target, name string, data []byte) (checkedBlob, error) {
	err := b.put(ctx, place, name, data, "", false)
	return checkedBlob{indexBlob{name, int64(len(data))}, indexChecksum(data)}, err
}

func (b *indexBuilder) writeSegment(ctx context.Context, place target, entries []indexEntry, pairs []uint64, now uint64) (segmentRef, error) {
	slices.Sort(pairs)
	postings := []byte(postingsMagic)
	var gramMap []byte
	var count, previousGram, previousOffset uint64
	for i := 0; i < len(pairs); {
		gram := pairs[i] >> 24
		end := i + 1
		for end < len(pairs) && pairs[end]>>24 == gram {
			end++
		}
		offset := uint64(len(postings))
		gramMap = binary.AppendUvarint(gramMap, gram-previousGram)
		gramMap = binary.AppendUvarint(gramMap, offset-previousOffset)
		previousGram, previousOffset = gram, offset
		count++
		postings = binary.AppendUvarint(postings, uint64(end-i))
		var previousID uint64
		for _, pair := range pairs[i:end] {
			id := pair & (maxIndexIDs - 1)
			postings = binary.AppendUvarint(postings, id-previousID)
			previousID = id
		}
		i = end
	}
	id := rand.Text()
	base := "seg/" + id
	if err := b.put(ctx, place, base+".postings", postings, "", false); err != nil {
		return segmentRef{}, err
	}
	ref := segmentRef{ID: id, Created: now, Objects: uint64(len(entries)), Grams: count}
	ref.Blobs.Postings = indexBlob{base + ".postings", int64(len(postings))}
	var err error
	ref.Blobs.Objects, err = b.writeChecked(ctx, place, base+".objects", encodeIndexEntries(entries))
	if err != nil {
		return segmentRef{}, err
	}
	gramData := append(binary.AppendUvarint([]byte(gramsMagic), count), gramMap...)
	ref.Blobs.Grams, err = b.writeChecked(ctx, place, base+".grams", gramData)
	if err != nil {
		return segmentRef{}, err
	}
	ref.Blobs.Sums, err = b.writeChecked(ctx, place, base+".sums", postingSums(postings))
	return ref, err
}
