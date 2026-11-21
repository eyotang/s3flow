package rg

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/eyotang/s3flow/internal/pkg"
)

func BenchmarkLiteralMatch(b *testing.B) {
	payload := bytes.Repeat([]byte("INFO request completed in 14ms\nERROR request timed out after 1000ms\n"), 1<<17)
	for _, pattern := range []string{"timed out", "^ERROR"} {
		b.Run(pattern, func(b *testing.B) {
			optimized := options{count: true, patterns: []string{pattern}}
			if err := optimized.compileMatchers(); err != nil {
				b.Fatal(err)
			}
			baseline := options{count: true, matchers: []func([]byte) bool{regexp.MustCompile(pattern).Match}}
			for _, tc := range []struct {
				name string
				opts options
			}{{"regexp", baseline}, {"optimized", optimized}} {
				b.Run(tc.name, func(b *testing.B) {
					b.SetBytes(int64(len(payload)))
					b.ReportAllocs()
					client := benchmarkClient{payload: payload}
					for b.Loop() {
						found, err := scanObject(context.Background(), client, target{"logs", "app.log"}, tc.opts, false, func(record []byte) error {
							if string(record) != "131072\n" {
								b.Fatalf("wrong count: %q", record)
							}
							return nil
						})
						if err != nil || !found {
							b.Fatalf("found=%t error=%v", found, err)
						}
					}
				})
			}
		})
	}
}

// firstResultWriter is called under search's output lock.
type firstResultWriter struct {
	start   time.Time
	latency time.Duration
	writes  int
	lines   int
}

func (w *firstResultWriter) Write(p []byte) (int, error) {
	if w.writes == 0 {
		w.latency = time.Since(w.start)
	}
	w.writes++
	w.lines += bytes.Count(p, []byte("\n"))
	return len(p), nil
}

func BenchmarkSearchPrefix(b *testing.B) {
	for _, tc := range []struct {
		name           string
		objects, lines int
		gzip           bool
	}{
		{"small", 256, 128, false},
		{"large", 8, 1 << 17, false},
		{"gzip", 32, 1 << 15, true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			raw := bytes.Repeat([]byte("INFO request completed in 14ms\nERROR request timed out after 1000ms\n"), tc.lines)
			payload, suffix := raw, ".log"
			if tc.gzip {
				var buf bytes.Buffer
				gz := gzip.NewWriter(&buf)
				if _, err := gz.Write(raw); err != nil {
					b.Fatal(err)
				}
				if err := gz.Close(); err != nil {
					b.Fatal(err)
				}
				payload, suffix = buf.Bytes(), ".log.gz"
			}
			// Serve two pages so the benchmark exercises the listing and worker pipeline.
			var pages [2]string
			for page := range pages {
				var xml strings.Builder
				fmt.Fprintf(&xml, "<ListBucketResult><IsTruncated>%t</IsTruncated>", page == 0)
				if page == 0 {
					xml.WriteString("<NextContinuationToken>next</NextContinuationToken>")
				}
				for i := 0; i < tc.objects/2; i++ {
					fmt.Fprintf(&xml, "<Contents><Key>logs/%d%s</Key><Size>%d</Size></Contents>", page*tc.objects/2+i, suffix, len(payload))
				}
				xml.WriteString("</ListBucketResult>")
				pages[page] = xml.String()
			}
			var objectGETs, downloadBytes, connections atomic.Int64
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("list-type") == "2" {
					w.Header().Set("Content-Type", "application/xml")
					page := 0
					if r.URL.Query().Get("continuation-token") == "next" {
						page = 1
						if tc.name == "small" {
							// Let the preceding wave of small GETs become idle.
							time.Sleep(20 * time.Millisecond)
						}
					}
					io.WriteString(w, pages[page])
					return
				}
				objectGETs.Add(1)
				time.Sleep(time.Millisecond)
				downloadBytes.Add(int64(len(payload)))
				w.Write(payload)
			}))
			server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
				if state == http.StateNew {
					connections.Add(1)
				}
			}
			server.Start()
			defer server.Close()
			ctx := context.Background()
			cfg := pkg.Config{Endpoint: server.URL, Region: "us-east-1", AccessKey: "test", SecretKey: "test", PathStyle: true}
			type variantConfig struct {
				name     string
				jobs     int
				tunePool bool
			}
			variants := []variantConfig{{"j=1", 1, true}, {"j=4", 4, true}, {"j=16", 16, true}}
			if tc.name == "small" {
				variants = append(variants,
					variantConfig{"j=32/default-pool", 32, false},
					variantConfig{"j=32/tuned-pool", 32, true},
				)
			}
			for _, variant := range variants {
				b.Run(variant.name, func(b *testing.B) {
					client, err := pkg.NewS3Client(ctx, cfg)
					if err != nil {
						b.Fatal(err)
					}
					if variant.tunePool {
						client = pkg.WithS3IdleConnections(client, variant.jobs+1, variant.jobs+1)
					}
					opts := options{jobs: variant.jobs, count: true, literal: true, patterns: []string{"timed out"}, targets: []target{{"logs-bucket", "logs/"}}}
					if err := opts.compileMatchers(); err != nil {
						b.Fatal(err)
					}
					b.SetBytes(int64(len(raw)) * int64(tc.objects))
					var first time.Duration
					var gets, downloaded, opened int64
					for b.Loop() {
						// Each CLI search starts with an empty connection pool.
						b.StopTimer()
						server.CloseClientConnections()
						beforeGETs, beforeBytes, beforeOpened := objectGETs.Load(), downloadBytes.Load(), connections.Load()
						b.StartTimer()
						output := firstResultWriter{start: time.Now()}
						var diagnostics bytes.Buffer
						if err := search(ctx, client, opts, &output, &diagnostics); err != nil {
							b.Fatalf("search failed: %v: %s", err, &diagnostics)
						}
						if output.writes != tc.objects {
							b.Fatalf("got %d of %d objects", output.writes, tc.objects)
						}
						first += output.latency
						gets += objectGETs.Load() - beforeGETs
						downloaded += downloadBytes.Load() - beforeBytes
						opened += connections.Load() - beforeOpened
					}
					b.ReportMetric(float64(first.Nanoseconds())/float64(b.N), "first-ns/op")
					b.ReportMetric(float64(gets)/float64(b.N), "GET/op")
					b.ReportMetric(float64(downloaded)/float64(b.N), "download-B/op")
					b.ReportMetric(float64(opened)/float64(b.N), "connections/op")
				})
			}
		})
	}
}

type inMemoryPrefix struct {
	benchmarkClient
	listing *s3.ListObjectsV2Output
}

func (p inMemoryPrefix) ListObjectsV2(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	return p.listing, nil
}

func BenchmarkSearchOutput(b *testing.B) {
	const objects, lines = 8, 1 << 14
	line := []byte("ERROR request timed out after 1000ms\n")
	payload := bytes.Repeat(line, lines)
	listing := &s3.ListObjectsV2Output{}
	for index := range objects {
		listing.Contents = append(listing.Contents, types.Object{Key: aws.String(fmt.Sprintf("logs/%d.log", index))})
	}
	client := inMemoryPrefix{benchmarkClient{payload: payload}, listing}
	opts := options{jobs: 8, numbers: true, patterns: []string{"^ERROR"}, targets: []target{{"logs-bucket", "logs/"}}}
	if err := opts.compileMatchers(); err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(payload)) * objects)
	b.ReportAllocs()
	var writes int
	for b.Loop() {
		output := firstResultWriter{start: time.Now()}
		if err := search(context.Background(), client, opts, &output, io.Discard); err != nil {
			b.Fatal(err)
		}
		if output.lines != objects*lines {
			b.Fatalf("got %d result lines", output.lines)
		}
		writes += output.writes
	}
	b.ReportMetric(float64(writes)/float64(b.N), "writes/op")
}
