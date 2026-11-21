package rg

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"
)

// The v2 wire format is documented in xiaofan-luan/s3rg's s3rg-index crate.
// Object IDs, gram codes and offsets are encoded as unsigned LEB128 values.
const (
	indexVersion  = 2
	indexDir      = ".s3rg/"
	indexMarker   = "INDEX"
	indexRootName = "root.json"
	objectsMagic  = "S3RGOBJ1"
	gramsMagic    = "S3RGGRM1"
	postingsMagic = "S3RGPST1"
	deadMagic     = "S3RGDED1"
	postingsBlock = 16 << 10
	maxIndexIDs   = 1 << 24
)

type indexSource struct {
	Root   string `json:"root"`
	Prefix string `json:"prefix"`
}

func (source indexSource) relative(object target) (string, bool) {
	if source.Root != "s3://"+object.bucket {
		return "", false
	}
	if source.Prefix == "" {
		return object.key, true
	}
	return strings.CutPrefix(object.key, source.Prefix+"/")
}

type indexRoot struct {
	Format     uint32            `json:"format"`
	Source     indexSource       `json:"source"`
	Decoders   map[string]uint32 `json:"decoders"`
	Generation uint64            `json:"generation"`
	Created    uint64            `json:"created"`
	Segments   []segmentRef      `json:"segments"`
	Retired    []retiredBlob     `json:"retired,omitempty"`
}

type retiredBlob struct {
	Key string `json:"key"`
	At  uint64 `json:"at"`
}

type indexBlob struct {
	Key string `json:"key"`
	Len int64  `json:"len"`
}

type checkedBlob struct {
	indexBlob
	SHA256 string `json:"sha256"`
}

type segmentBlobs struct {
	Objects  checkedBlob `json:"objects"`
	Grams    checkedBlob `json:"grams"`
	Postings indexBlob   `json:"postings"`
	Sums     checkedBlob `json:"sums"`
}

type segmentRef struct {
	ID      string       `json:"id"`
	Created uint64       `json:"created"`
	Objects uint64       `json:"objects"`
	Grams   uint64       `json:"grams"`
	Blobs   segmentBlobs `json:"blobs"`
	Dead    *checkedBlob `json:"dead,omitempty"`
}

func (segment segmentRef) keys() []string {
	keys := []string{segment.Blobs.Objects.Key, segment.Blobs.Grams.Key, segment.Blobs.Postings.Key, segment.Blobs.Sums.Key}
	if segment.Dead != nil {
		keys = append(keys, segment.Dead.Key)
	}
	return keys
}

func parseIndexRoot(data []byte) (indexRoot, error) {
	var root indexRoot
	if err := json.Unmarshal(data, &root); err != nil {
		return root, fmt.Errorf("invalid index root: %w", err)
	}
	if root.Format != indexVersion {
		return root, fmt.Errorf("index format %d is unsupported (expected %d)", root.Format, indexVersion)
	}
	if root.Source.Root == "" || root.Decoders == nil || root.Segments == nil {
		return root, errors.New("invalid index root")
	}
	for _, segment := range root.Segments {
		if segment.ID == "" || segment.Objects >= maxIndexIDs || segment.Grams > maxIndexIDs {
			return root, errors.New("invalid index segment")
		}
		for _, key := range segment.keys() {
			if !validSegmentKey(key) {
				return root, fmt.Errorf("invalid segment key %q", key)
			}
		}
	}
	for _, retired := range root.Retired {
		if !validSegmentKey(retired.Key) {
			return root, fmt.Errorf("invalid retired segment key %q", retired.Key)
		}
	}
	return root, nil
}

func validSegmentKey(key string) bool {
	if !strings.HasPrefix(key, "seg/") {
		return false
	}
	for part := range strings.SplitSeq(key, "/") {
		if part == "" || part == "." || part == ".." {
			return false
		}
	}
	return true
}

type indexEntry struct {
	key      string
	size     uint64
	etag     string
	decoding uint8
	ok       bool
}

func (root indexRoot) decoderMatches(decoding uint8) bool {
	switch decoding {
	case decodeRaw:
		return true
	case decodeGzip:
		return root.Decoders["gzip"] == 1
	default:
		return false
	}
}

type indexDecoder struct {
	data []byte
	err  error
}

func decodeIndexBlob(data []byte, magic string) indexDecoder {
	if len(data) < len(magic) || string(data[:len(magic)]) != magic {
		return indexDecoder{err: errors.New("invalid index blob header")}
	}
	return indexDecoder{data: data[len(magic):]}
}

func (d *indexDecoder) number() uint64 {
	if d.err != nil {
		return 0
	}
	value, n := binary.Uvarint(d.data)
	if n <= 0 {
		d.err = errors.New("invalid index integer")
		return 0
	}
	d.data = d.data[n:]
	return value
}

func (d *indexDecoder) take(n uint64) []byte {
	if d.err != nil {
		return nil
	}
	if n > uint64(len(d.data)) {
		d.err = errors.New("truncated index blob")
		return nil
	}
	value := d.data[:n]
	d.data = d.data[n:]
	return value
}

func (d *indexDecoder) finish() error {
	if d.err != nil {
		return d.err
	}
	if len(d.data) != 0 {
		return errors.New("unexpected data after index records")
	}
	return nil
}

func parseIndexEntries(data []byte, expected uint64) ([]indexEntry, error) {
	d := decodeIndexBlob(data, objectsMagic)
	count := d.number()
	if count != expected || count >= maxIndexIDs || count > uint64(len(d.data)/5) {
		return nil, errors.New("invalid index object count")
	}
	entries := make([]indexEntry, 0, count)
	for range count {
		key := string(d.take(d.number()))
		size := d.number()
		etag := string(d.take(d.number()))
		flags := d.take(2)
		if d.err != nil {
			return nil, d.err
		}
		if flags[0] > 2 || flags[1] > 1 || !utf8.ValidString(key) || !utf8.ValidString(etag) ||
			(len(entries) > 0 && key <= entries[len(entries)-1].key) {
			return nil, errors.New("invalid index object entry")
		}
		entries = append(entries, indexEntry{key: key, size: size, etag: etag, decoding: flags[0], ok: flags[1] == 0})
	}
	return entries, d.finish()
}

func encodeIndexEntries(entries []indexEntry) []byte {
	data := binary.AppendUvarint([]byte(objectsMagic), uint64(len(entries)))
	for _, entry := range entries {
		data = append(binary.AppendUvarint(data, uint64(len(entry.key))), entry.key...)
		data = binary.AppendUvarint(data, entry.size)
		data = append(binary.AppendUvarint(data, uint64(len(entry.etag))), entry.etag...)
		status := byte(1)
		if entry.ok {
			status = 0
		}
		data = append(data, entry.decoding, status)
	}
	return data
}

func (d *indexDecoder) ids(objects int) ([]uint32, error) {
	count := d.number()
	if count > uint64(objects) || count > uint64(len(d.data)) {
		return nil, errors.New("invalid index ID count")
	}
	ids := make([]uint32, 0, count)
	var id uint64
	for i := range count {
		delta := d.number()
		if (i > 0 && delta == 0) || delta >= uint64(objects)-id {
			return nil, errors.New("invalid index object ID")
		}
		id += delta
		ids = append(ids, uint32(id))
	}
	return ids, d.finish()
}

func appendIndexIDs(data []byte, ids []uint32) []byte {
	data = binary.AppendUvarint(data, uint64(len(ids)))
	var previous uint32
	for _, id := range ids {
		data = binary.AppendUvarint(data, uint64(id-previous))
		previous = id
	}
	return data
}

type postingRange struct{ start, end int64 }

func parseGramRanges(data []byte, length int64, expected uint64, wanted map[uint32]struct{}) (map[uint32]postingRange, error) {
	d := decodeIndexBlob(data, gramsMagic)
	count := d.number()
	if count != expected || count > maxIndexIDs || count > uint64(len(d.data)/2) || length < int64(len(postingsMagic)) || length >= 1<<40 {
		return nil, errors.New("invalid gram table size")
	}
	var gram, offset uint64
	ranges := make(map[uint32]postingRange)
	for i := range count {
		code, step := d.number(), d.number()
		if code >= maxIndexIDs-gram || (i > 0 && code == 0) || step == 0 || step >= uint64(length)-offset {
			return nil, errors.New("invalid gram table entry")
		}
		if i > 0 {
			if _, ok := wanted[uint32(gram)]; ok {
				ranges[uint32(gram)] = postingRange{int64(offset), int64(offset + step)}
			}
		}
		gram += code
		offset += step
		if i == 0 && offset != uint64(len(postingsMagic)) {
			return nil, errors.New("invalid first posting offset")
		}
	}
	if _, ok := wanted[uint32(gram)]; count > 0 && ok {
		ranges[uint32(gram)] = postingRange{int64(offset), length}
	}
	if count == 0 && length != int64(len(postingsMagic)) {
		return nil, errors.New("postings without gram entries")
	}
	return ranges, d.finish()
}

func indexChecksum(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func postingSums(data []byte) []byte {
	var sums []byte
	for block := range slices.Chunk(data, postingsBlock) {
		sum := sha256.Sum256(block)
		sums = append(sums, sum[:]...)
	}
	return sums
}
