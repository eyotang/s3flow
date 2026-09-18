package pkg_test

import (
	"github.com/eyotang/s3flow/internal/pkg"
	"reflect"
	"strings"
	"testing"
)

func TestParseS3URIDecodesEscapedObjectKey(t *testing.T) {
	const source = "s3://checkpoint/%E7%BA%BF%E4%B8%8A%E6%9C%8D_rm42_p4.internal.example/2026-09-13_040000/master.ckp.147.gz"
	const wantKey = "线上服_rm42_p4.internal.example/2026-09-13_040000/master.ckp.147.gz"

	bucket, key, err := pkg.ParseS3URI(source)
	if err != nil {
		t.Fatalf("parse escaped S3 URI: %v", err)
	}
	if bucket != "checkpoint" {
		t.Fatalf("bucket = %q, want checkpoint", bucket)
	}
	if key != wantKey {
		t.Fatalf("key = %q, want decoded key %q", key, wantKey)
	}
}

func TestParseS3URIPathEscapingRules(t *testing.T) {
	tests := []struct {
		name    string
		source  string
		wantKey string
	}{
		{
			name:    "native unicode",
			source:  "s3://checkpoint/线上服/object.gz",
			wantKey: "线上服/object.gz",
		},
		{
			name:    "plus is not a space",
			source:  "s3://checkpoint/folder+name/file%20name.gz",
			wantKey: "folder+name/file name.gz",
		},
		{
			name:    "escaped slash and percent",
			source:  "s3://checkpoint/folder%2Fnested/100%25.gz",
			wantKey: "folder/nested/100%.gz",
		},
		{
			name:    "escaped percent sequence stays literal",
			source:  "s3://checkpoint/literal%252Fname.gz",
			wantKey: "literal%2Fname.gz",
		},
		{
			name:    "escaped query and fragment characters",
			source:  "s3://checkpoint/literal%3Fquery%23fragment.gz",
			wantKey: "literal?query#fragment.gz",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, key, err := pkg.ParseS3URI(test.source)
			if err != nil {
				t.Fatalf("parse S3 URI: %v", err)
			}
			if key != test.wantKey {
				t.Fatalf("key = %q, want %q", key, test.wantKey)
			}
		})
	}
}

func TestParseS3URIRejectsInvalidPercentEncoding(t *testing.T) {
	for _, source := range []string{
		"s3://checkpoint/broken%",
		"s3://checkpoint/broken%2",
		"s3://checkpoint/broken%GG",
	} {
		_, _, err := pkg.ParseS3URI(source)
		if err == nil || !strings.Contains(err.Error(), "无效的 percent 编码") {
			t.Fatalf("pkg.ParseS3URI(%q) error = %v, want invalid percent encoding", source, err)
		}
	}
}

func TestPlanPartsBoundaries(t *testing.T) {
	tests := []struct {
		name     string
		size     int64
		partSize int64
		want     []pkg.Part
	}{
		{name: "empty object", size: 0, partSize: 4, want: nil},
		{name: "single byte", size: 1, partSize: 4, want: []pkg.Part{{Index: 0, Start: 0, End: 0}}},
		{name: "smaller than one part", size: 3, partSize: 4, want: []pkg.Part{{Index: 0, Start: 0, End: 2}}},
		{name: "exact multiple", size: 8, partSize: 4, want: []pkg.Part{
			{Index: 0, Start: 0, End: 3},
			{Index: 1, Start: 4, End: 7},
		}},
		{name: "final partial part", size: 10, partSize: 4, want: []pkg.Part{
			{Index: 0, Start: 0, End: 3},
			{Index: 1, Start: 4, End: 7},
			{Index: 2, Start: 8, End: 9},
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := pkg.PlanParts(test.size, test.partSize)
			if err != nil {
				t.Fatalf("plan parts: %v", err)
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("unexpected plan:\n got: %#v\nwant: %#v", got, test.want)
			}
		})
	}
}

func TestPlanPartsLimitsCountAndAvoidsInt64Overflow(t *testing.T) {
	parts, err := pkg.PlanParts(pkg.MaxParts+1, 1)
	if err != nil {
		t.Fatalf("plan capped parts: %v", err)
	}
	if len(parts) > int(pkg.MaxParts) {
		t.Fatalf("expected at most %d parts, got %d", pkg.MaxParts, len(parts))
	}
	if len(parts) != 5001 || parts[0] != (pkg.Part{Index: 0, Start: 0, End: 1}) {
		t.Fatalf("unexpected automatically enlarged part plan: first=%#v count=%d", parts[0], len(parts))
	}
	if last := parts[len(parts)-1]; last.End != pkg.MaxParts {
		t.Fatalf("expected final byte %d to be covered, got last part %#v", pkg.MaxParts, last)
	}

	const maxInt64 int64 = 1<<63 - 1
	parts, err = pkg.PlanParts(maxInt64, maxInt64)
	if err != nil {
		t.Fatalf("plan MaxInt64 object: %v", err)
	}
	want := []pkg.Part{{Index: 0, Start: 0, End: maxInt64 - 1}}
	if !reflect.DeepEqual(parts, want) {
		t.Fatalf("unexpected MaxInt64 plan: got %#v want %#v", parts, want)
	}
}

func TestPlanPartsRejectsInvalidSizes(t *testing.T) {
	if _, err := pkg.PlanParts(-1, 4); err == nil {
		t.Fatal("expected negative object size to fail")
	}
	if _, err := pkg.PlanParts(1, 0); err == nil {
		t.Fatal("expected zero part size to fail")
	}
}
