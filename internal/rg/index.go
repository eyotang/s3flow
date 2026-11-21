package rg

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const (
	maxIndexRootBytes = 4 << 20
	maxIndexBlobBytes = 256 << 20
)

type objectMeta struct {
	target
	size *int64
	etag *string
}

func (object objectMeta) hasRevision() bool {
	return object.size != nil && *object.size >= 0 && aws.ToString(object.etag) != ""
}

func (entry indexEntry) describes(object objectMeta, decoding uint8) bool {
	return object.hasRevision() && entry.etag == *object.etag && entry.size == uint64(*object.size) && entry.decoding == decoding
}

func indexPlace(prefix target) target {
	if prefix.key != "" && !strings.HasSuffix(prefix.key, "/") {
		prefix.key += "/"
	}
	prefix.key += indexDir
	return prefix
}

func parentPrefix(key string) string {
	return key[:strings.LastIndexByte(strings.TrimSuffix(key, "/"), '/')+1]
}

func ancestorIndexes(t target) map[target]bool {
	places := make(map[target]bool)
	for range 32 {
		places[indexPlace(t)] = true
		if t.key == "" {
			break
		}
		t.key = parentPrefix(t.key)
	}
	return places
}

func s3Status(err error, code int) bool {
	var response interface{ HTTPStatusCode() int }
	return errors.As(err, &response) && response.HTTPStatusCode() == code
}

func s3Missing(err error) bool {
	var noKey *types.NoSuchKey
	var notFound *types.NotFound
	return errors.As(err, &noKey) || errors.As(err, &notFound) || s3Status(err, 404)
}

type indexReader struct {
	client                  searchClient
	bytes, requests, ranges int64
}

func (r *indexReader) get(ctx context.Context, place target, name string, limit int64, requestedRange string) ([]byte, string, error) {
	input := &s3.GetObjectInput{Bucket: aws.String(place.bucket), Key: aws.String(place.key + name)}
	if requestedRange != "" {
		input.Range = aws.String(requestedRange)
	}
	response, err := r.client.GetObject(ctx, input, disableMissingChecksumWarning)
	if err != nil {
		return nil, "", err
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, limit+1))
	r.bytes += int64(len(data))
	r.requests++
	if requestedRange != "" {
		r.ranges++
	}
	if err != nil {
		return nil, "", err
	}
	if int64(len(data)) > limit {
		return nil, "", fmt.Errorf("index blob %s exceeds %d bytes", name, limit)
	}
	return data, aws.ToString(response.ETag), nil
}

func (r *indexReader) root(ctx context.Context, place target) ([]byte, string, error) {
	return r.get(ctx, place, indexRootName, maxIndexRootBytes, "")
}

func (r *indexReader) blob(ctx context.Context, place target, blob checkedBlob) ([]byte, error) {
	if blob.Len < 0 || blob.Len > maxIndexBlobBytes {
		return nil, fmt.Errorf("invalid index blob length for %s", blob.Key)
	}
	data, _, err := r.get(ctx, place, blob.Key, blob.Len, "")
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", blob.Key, err)
	}
	if int64(len(data)) != blob.Len || indexChecksum(data) != blob.SHA256 {
		return nil, fmt.Errorf("index checksum or length mismatch for %s", blob.Key)
	}
	return data, nil
}

type objectTable struct {
	entries []indexEntry
	dead    []uint32
}

func (r *indexReader) table(ctx context.Context, place target, segment segmentRef) (objectTable, error) {
	var table objectTable
	data, err := r.blob(ctx, place, segment.Blobs.Objects)
	if err != nil {
		return table, err
	}
	table.entries, err = parseIndexEntries(data, segment.Objects)
	if err != nil || segment.Dead == nil {
		return table, err
	}
	data, err = r.blob(ctx, place, *segment.Dead)
	if err != nil {
		return table, err
	}
	d := decodeIndexBlob(data, deadMagic)
	table.dead, err = d.ids(len(table.entries))
	return table, err
}

func (r *indexReader) postings(ctx context.Context, place target, segment segmentRef, wanted map[uint32]struct{}) (map[uint32][]uint32, error) {
	data, err := r.blob(ctx, place, segment.Blobs.Grams)
	if err != nil {
		return nil, err
	}
	blob := segment.Blobs.Postings
	lists, err := parseGramRanges(data, blob.Len, segment.Grams, wanted)
	if err != nil || len(lists) == 0 {
		return nil, err
	}
	sums, err := r.blob(ctx, place, segment.Blobs.Sums)
	if err != nil {
		return nil, err
	}
	if int64(len(sums)) != (blob.Len+postingsBlock-1)/postingsBlock*sha256.Size {
		return nil, errors.New("invalid posting block checksums")
	}
	grams := make([]uint32, 0, len(lists))
	for gram := range lists {
		grams = append(grams, gram)
	}
	slices.Sort(grams)
	parts := postingParts(grams, lists, blob.Len)
	found := make(map[uint32][]uint32, len(lists))
	for _, part := range parts {
		if part.end-part.start > maxIndexBlobBytes {
			return nil, errors.New("index posting range is too large")
		}
		request := fmt.Sprintf("bytes=%d-%d", part.start, part.end-1)
		data, _, err := r.get(ctx, place, blob.Key, part.end-part.start, request)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", blob.Key, err)
		}
		first := part.start / postingsBlock * sha256.Size
		last := (part.end + postingsBlock - 1) / postingsBlock * sha256.Size
		if int64(len(data)) != part.end-part.start || !bytes.Equal(postingSums(data), sums[first:last]) ||
			(part.start == 0 && !bytes.HasPrefix(data, []byte(postingsMagic))) {
			return nil, fmt.Errorf("index posting checksum or length mismatch for %s", blob.Key)
		}
		for len(grams) > 0 && lists[grams[0]].start < part.end {
			gram := grams[0]
			list := lists[gram]
			d := indexDecoder{data: data[list.start-part.start : list.end-part.start]}
			ids, err := d.ids(int(segment.Objects))
			if err != nil || len(ids) == 0 {
				return nil, fmt.Errorf("invalid index posting list in %s", blob.Key)
			}
			found[gram] = ids
			grams = grams[1:]
		}
	}
	return found, nil
}

func postingParts(grams []uint32, lists map[uint32]postingRange, length int64) []postingRange {
	var parts []postingRange
	for _, gram := range grams {
		list := lists[gram]
		start := list.start / postingsBlock * postingsBlock
		end := min(length, (list.end+postingsBlock-1)/postingsBlock*postingsBlock)
		if len(parts) > 0 && start <= parts[len(parts)-1].end+4*postingsBlock {
			parts[len(parts)-1].end = end
		} else {
			parts = append(parts, postingRange{start, end})
		}
	}
	var requested int64
	for _, part := range parts {
		requested += part.end - part.start
	}
	if requested*2 >= length && length <= maxIndexBlobBytes {
		return []postingRange{{0, length}}
	}
	return parts
}

type loadedSegment struct {
	objectTable
	postings map[uint32][]uint32
}

type loadedIndex struct {
	root     indexRoot
	segments []loadedSegment
}

func (r *indexReader) load(ctx context.Context, place target, wanted map[uint32]struct{}, explicit bool) (*loadedIndex, error) {
	data, _, err := r.root(ctx, place)
	if s3Missing(err) || (!explicit && s3Status(err, 403)) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	root, err := parseIndexRoot(data)
	if err != nil {
		return nil, err
	}
	index := &loadedIndex{root: root}
	for _, ref := range root.Segments {
		table, err := r.table(ctx, place, ref)
		if err != nil {
			return nil, err
		}
		postings, err := r.postings(ctx, place, ref, wanted)
		if err != nil {
			return nil, err
		}
		index.segments = append(index.segments, loadedSegment{table, postings})
	}
	return index, nil
}

func (idx *loadedIndex) verdict(object objectMeta, decoding uint8, query *gramCondition) (covered, skip bool) {
	key, under := idx.root.Source.relative(object.target)
	if !under || !idx.root.decoderMatches(decoding) {
		return false, false
	}
	for _, segment := range slices.Backward(idx.segments) {
		pos, found := slices.BinarySearchFunc(segment.entries, key, func(entry indexEntry, key string) int {
			return strings.Compare(entry.key, key)
		})
		id := uint32(pos)
		_, dead := slices.BinarySearch(segment.dead, id)
		if !found || dead {
			continue
		}
		entry := segment.entries[pos]
		if entry.ok && entry.describes(object, decoding) {
			return true, !query.possible(segment.postings, id)
		}
		return false, false
	}
	return false, false
}

type searchIndex struct {
	reader    indexReader
	query     *gramCondition
	wanted    map[uint32]struct{}
	loaded    map[target]*loadedIndex
	explicit  *target
	report    func(target, error)
	listed    int
	skipped   int
	uncovered int
}

func newSearchIndex(client searchClient, opts options, report func(target, error)) *searchIndex {
	idx := &searchIndex{reader: indexReader{client: client}, explicit: opts.indexAt, report: report}
	if opts.gramQuery != nil {
		idx.wanted = make(map[uint32]struct{})
		opts.gramQuery.terms(idx.wanted)
		if len(idx.wanted) <= 4096 {
			idx.query = opts.gramQuery
			idx.loaded = make(map[target]*loadedIndex)
		}
	}
	return idx
}

func (idx *searchIndex) check(ctx context.Context, place target, object objectMeta, decoding uint8) (covered, skip bool) {
	index, cached := idx.loaded[place]
	if !cached {
		var err error
		index, err = idx.reader.load(ctx, place, idx.wanted, idx.explicit != nil)
		if err != nil && ctx.Err() == nil {
			idx.report(place, err)
		}
		idx.loaded[place] = index
	}
	if index == nil {
		return false, false
	}
	return index.verdict(object, decoding, idx.query)
}

func (idx *searchIndex) skip(ctx context.Context, places map[target]bool, object objectMeta, decoding uint8) bool {
	idx.listed++
	if idx.query != nil && object.hasRevision() {
		if idx.explicit != nil {
			covered, skip := idx.check(ctx, indexPlace(*idx.explicit), object, decoding)
			if covered {
				return idx.record(skip)
			}
		} else {
			for prefix := parentPrefix(object.key); ; prefix = parentPrefix(prefix) {
				place := indexPlace(target{object.bucket, prefix})
				if places[place] {
					covered, skip := idx.check(ctx, place, object, decoding)
					if covered {
						return idx.record(skip)
					}
				}
				if prefix == "" {
					break
				}
			}
		}
	}
	idx.uncovered++
	return false
}

func (idx *searchIndex) record(skip bool) bool {
	if skip {
		idx.skipped++
	}
	return skip
}

func (idx *searchIndex) printStats(stderr io.Writer) {
	fmt.Fprintf(stderr, "s3rg: index: %d objects listed, %d skipped by index, %d searched, %d not covered\n",
		idx.listed, idx.skipped, idx.listed-idx.skipped, idx.uncovered)
	if idx.reader.requests > 0 {
		fmt.Fprintf(stderr, "s3rg: index: %d bytes of index read in %d requests, %d of them for parts of the postings\n",
			idx.reader.bytes, idx.reader.requests, idx.reader.ranges)
	}
}

// A directory named .s3rg is reserved only when it has an INDEX marker.
// The caller passes the path below its listing prefix, so an explicitly
// requested index directory can still be searched as ordinary data.
func markedIndex(ctx context.Context, client searchClient, known map[target]bool, object target, relative string) (target, bool) {
	for rest := relative; ; {
		name, suffix, more := strings.Cut(rest, "/")
		if !more {
			return target{}, false
		}
		if name == strings.TrimSuffix(indexDir, "/") {
			place := target{object.bucket, object.key[:len(object.key)-len(suffix)]}
			if suffix == indexMarker {
				known[place] = true
			}
			marked, cached := known[place]
			if !cached {
				_, err := client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(place.bucket), Key: aws.String(place.key + indexMarker)})
				marked = err == nil
				known[place] = marked
			}
			if marked {
				return place, true
			}
		}
		rest = suffix
	}
}
