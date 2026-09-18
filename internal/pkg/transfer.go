package pkg

import (
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

const (
	DefaultRegion      = "cn-sh"
	DefaultPartSize    = int64(128 << 20)
	DefaultConcurrency = 16
	MaxConcurrency     = 128
	MaxParts           = int64(10000)
)

type Part struct {
	Index int
	Start int64
	End   int64
}

func (p Part) Size() int64 { return p.End - p.Start + 1 }

type ByteSizeFlag struct{ Value int64 }

func (v *ByteSizeFlag) String() string { return FormatBytes(v.Value) }

func (v *ByteSizeFlag) Set(raw string) error {
	n, err := ParseByteSize(raw)
	if err != nil {
		return err
	}
	v.Value = n
	return nil
}

func ParseByteSize(raw string) (int64, error) {
	value := strings.ToUpper(strings.TrimSpace(raw))
	units := []struct {
		suffix     string
		multiplier float64
	}{
		{"GIB", 1 << 30}, {"GB", 1e9},
		{"MIB", 1 << 20}, {"MB", 1e6},
		{"KIB", 1 << 10}, {"KB", 1e3},
		{"B", 1},
	}
	multiplier := float64(1)
	for _, unit := range units {
		if strings.HasSuffix(value, unit.suffix) {
			value = strings.TrimSpace(strings.TrimSuffix(value, unit.suffix))
			multiplier = unit.multiplier
			break
		}
	}
	n, err := strconv.ParseFloat(value, 64)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("无效的字节大小 %q", raw)
	}
	result := int64(n * multiplier)
	if result <= 0 {
		return 0, fmt.Errorf("无效的字节大小 %q", raw)
	}
	return result, nil
}

func FormatS3URIForDisplay(bucket, key string) string {
	displayURL := &url.URL{
		Scheme: "s3",
		Host:   bucket,
		Path:   "/" + key,
	}
	return displayURL.String()
}

func PlanParts(size, requestedPartSize int64) ([]Part, error) {
	if size < 0 {
		return nil, errors.New("对象大小不能为负数")
	}
	if requestedPartSize <= 0 {
		return nil, errors.New("分片大小必须大于 0")
	}
	if size == 0 {
		return nil, nil
	}
	partSize := requestedPartSize
	minimum := size / MaxParts
	if size%MaxParts != 0 {
		minimum++
	}
	if minimum > partSize {
		partSize = minimum
	}
	count := size / partSize
	if size%partSize != 0 {
		count++
	}
	parts := make([]Part, 0, count)
	for start, index := int64(0), 0; start < size; index++ {
		length := min(partSize, size-start)
		end := start + length - 1
		parts = append(parts, Part{Index: index, Start: start, End: end})
		start += length
	}
	return parts, nil
}

func FormatBytes(value int64) string {
	if value < 1024 {
		return fmt.Sprintf("%d B", value)
	}
	units := []string{"KiB", "MiB", "GiB", "TiB", "PiB"}
	size := float64(value)
	unit := "B"
	for _, candidate := range units {
		size /= 1024
		unit = candidate
		if size < 1024 {
			break
		}
	}
	return fmt.Sprintf("%.1f %s", size, unit)
}
