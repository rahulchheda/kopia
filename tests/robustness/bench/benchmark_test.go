package bench

import (
	"context"
	"flag"
	"fmt"
	"path"
	"strconv"
	"strings"
	"testing"

	engine "github.com/kopia/kopia/tests/robustness/test_engine"
)

const (
	dataSubPath     = "bench/robustness-data"
	metadataSubPath = "bench/robustness-metadata"
)

var (
	repoPathPrefix = flag.String("repo-path-prefix", "", "Point the robustness tests at this path prefix")
)

func BenchmarkContent0Metadata0Dedup100(b *testing.B)     { benchmarkCacheSize(0, 0, 100, b) }
func BenchmarkContent500Metadata0Dedup100(b *testing.B)   { benchmarkCacheSize(500, 0, 100, b) }
func BenchmarkContent0Metadata500Dedup100(b *testing.B)   { benchmarkCacheSize(0, 500, 100, b) }
func BenchmarkContent500Metadata500Dedup100(b *testing.B) { benchmarkCacheSize(500, 500, 100, b) }
func BenchmarkContent0Metadata0Dedup0(b *testing.B)       { benchmarkCacheSize(0, 0, 0, b) }
func BenchmarkContent500Metadata0Dedup0(b *testing.B)     { benchmarkCacheSize(500, 0, 0, b) }
func BenchmarkContent0Metadata500Dedup0(b *testing.B)     { benchmarkCacheSize(0, 500, 0, b) }
func BenchmarkContent500Metadata500Dedup0(b *testing.B)   { benchmarkCacheSize(500, 500, 0, b) }

func benchmarkCacheSize(contentCacheSizeMB, metadataCacheSizeMB, dedupPcnt int, b *testing.B) {
	eng, err := engine.NewEngine("")
	if err != nil {
		b.Fatalf("error on engine creation: %s\n", err)
	}

	defer eng.Cleanup()

	dataRepoPath := path.Join(*repoPathPrefix, dataSubPath)
	metadataRepoPath := path.Join(*repoPathPrefix, metadataSubPath)

	// Initialize the engine, connecting it to the repositories
	err = eng.Init(context.Background(), dataRepoPath, metadataRepoPath)
	if err != nil {
		if !strings.Contains(err.Error(), "errors verifying snapshot metadata") {
			b.Fatalf("error initializing engine for S3: %s\n", err)
		}
	}

	fileSize := 4 * 1024 * 1024
	numFiles := 1000

	wrOpts := map[string]string{
		engine.MaxDirDepthField:         strconv.Itoa(1),
		engine.MaxFileSizeField:         strconv.Itoa(fileSize),
		engine.MinFileSizeField:         strconv.Itoa(fileSize),
		engine.MaxNumFilesPerWriteField: strconv.Itoa(numFiles),
		engine.MinNumFilesPerWriteField: strconv.Itoa(numFiles),
		engine.MaxDedupePercentField:    strconv.Itoa(dedupPcnt),
		engine.MinDedupePercentField:    strconv.Itoa(dedupPcnt),
	}

	_, err = eng.ExecAction(engine.WriteRandomFilesActionKey, wrOpts)
	if err != nil {
		b.Fatalf("write error: %s", err)
	}

	_, _, err = eng.TestRepo.Run("cache", "set", "--content-cache-size-mb", strconv.Itoa(contentCacheSizeMB))
	if err != nil {
		b.Fatalf("error setting content cache size: %s", err)
	}

	_, _, err = eng.TestRepo.Run("cache", "set", "--metadata-cache-size-mb", strconv.Itoa(metadataCacheSizeMB))
	if err != nil {
		b.Fatalf("error setting metadata cache size: %s", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := eng.TestRepo.CreateSnapshot(eng.FileWriter.LocalDataDir)
		if err != nil {
			b.Fatalf("snapshot error: %s", err)
		}

		_, _, err = eng.TestRepo.Run("cache", "clear")
		if err != nil {
			fmt.Printf("error clearing cache: %s\n", err)
		}
	}

	_, _, err = eng.TestRepo.Run("cache", "clear")
	if err != nil {
		fmt.Printf("error clearing cache: %s\n", err)
	}
}
