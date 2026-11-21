package rg

import (
	"io"
	"strconv"
)

const maxOutputBatch = 32 << 10

// lineBatch combines complete output records from one object. Its first match
// is written immediately and subsequent records are flushed before reading
// more input, so a slow download cannot hold already matched lines.
type lineBatch struct {
	data    []byte
	write   func([]byte) error
	started bool
}

func (b *lineBatch) add(prefix string, number int64, line []byte) error {
	size := len(prefix) + len(line) + 1
	if number != 0 {
		size += 20 // Up to 19 digits and a colon.
	}
	if len(b.data)+size > maxOutputBatch {
		if err := b.flush(); err != nil {
			return err
		}
	}
	if size > maxOutputBatch {
		b.started = true
		return b.write(appendRecord(make([]byte, 0, size), prefix, number, line))
	}
	b.data = appendRecord(b.data, prefix, number, line)
	if !b.started {
		return b.flush()
	}
	return nil
}

func appendRecord(out []byte, prefix string, number int64, line []byte) []byte {
	out = append(out, prefix...)
	if number != 0 {
		out = strconv.AppendInt(out, number, 10)
		out = append(out, ':')
	}
	out = append(out, line...)
	return append(out, '\n')
}

func (b *lineBatch) flush() error {
	if len(b.data) == 0 {
		return nil
	}
	data := b.data
	b.data = b.data[:0]
	b.started = true
	return b.write(data)
}

type flushBeforeRead struct {
	io.Reader
	flush func() error
}

func (r flushBeforeRead) Read(p []byte) (int, error) {
	if err := r.flush(); err != nil {
		return 0, err
	}
	return r.Reader.Read(p)
}
