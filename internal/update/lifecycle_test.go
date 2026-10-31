package update

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// Run from a disposable executable: replacing the test runner itself would
// invalidate the build cache. The child exercises the actual installation and
// progress shutdown, then must exit without a signal or additional input.
func TestUpdateReplacesRunningExecutableAndExits(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("requires a Windows executable fixture and post-exit cleanup")
	}
	const marker = "S3FLOW_UPDATE_LIFECYCLE_CHILD"
	if os.Getenv(marker) == "1" {
		var archive bytes.Buffer
		gz := gzip.NewWriter(&archive)
		tw := tar.NewWriter(gz)
		content := []byte("updated executable")
		tw.WriteHeader(&tar.Header{Name: "release/s3get", Mode: 0755, Size: int64(len(content))})
		tw.Write(content)
		tw.Close()
		gz.Close()
		name := fmt.Sprintf("s3flow_v1.2.3_%s_%s.tar.gz", runtime.GOOS, runtime.GOARCH)
		sum := sha256.Sum256(archive.Bytes())
		sums := fmt.Sprintf("%x  %s\n", sum, name)
		client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			data := archive.Bytes()
			if strings.HasSuffix(req.URL.Path, "/checksums") {
				data = []byte(sums)
			}
			return &http.Response{StatusCode: 200, ContentLength: int64(len(data)), Body: io.NopCloser(bytes.NewReader(data))}, nil
		})}
		r := release{Tag: "v1.2.3", Assets: []asset{{Name: "checksums.txt", URL: api + "/checksums"}, {Name: name, URL: api + "/archive"}}}
		report, finish := showProgress(io.Discard)
		err := install(context.Background(), client, r, "", "s3get", report)
		finish()
		if err != nil {
			t.Fatal(err)
		}
		fmt.Println("更新完成，请重新运行 s3get")
		return
	}
	original, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(original)
	if err != nil {
		t.Fatal(err)
	}
	executable := filepath.Join(t.TempDir(), "updater")
	if err = os.WriteFile(executable, data, 0755); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	child := exec.CommandContext(ctx, executable, "-test.run=^TestUpdateReplacesRunningExecutableAndExits$")
	child.Env = append(os.Environ(), marker+"=1")
	output, err := child.CombinedOutput()
	if err != nil {
		t.Fatalf("updater did not exit successfully: %v\n%s", err, output)
	}
	if !strings.Contains(string(output), "更新完成，请重新运行 s3get") {
		t.Fatalf("missing completion: %s", output)
	}
	installed, err := os.ReadFile(executable)
	if err != nil || string(installed) != "updated executable" {
		t.Fatalf("replacement=%q err=%v", installed, err)
	}
}
