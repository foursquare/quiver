// Copyright (C) 2015 Foursquare Labs Inc.

package hfile

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path"
	"strings"
	"time"

	"github.com/foursquare/fsgo/report"
)

type LoadMethod int

const (
	CopiedToMem LoadMethod = iota
	MemlockFile
	OnDisk
)

type CollectionConfig struct {
	// The Name of the collection.
	Name string

	// The Hfile itself.
	SourcePath string

	// A local copy of SourcePath, if SourcePath is remote, otherwise the same as SourcePath.
	LocalPath string

	// If the local path has already been read, a cache to avoid re-reading.
	cachedContent *[]byte

	// If the collection data should be kept in-memory (via mlock).
	LoadMethod LoadMethod

	// Should operations on this collection emit verbose debug output.
	Debug bool

	// This "collection" may, in fact, be a partition (subset) of some larger (sharded) collection.
	ParentName      string
	ShardFunction   string
	Partition       string
	TotalPartitions string
}

type CollectionSet struct {
	Collections map[string]*Reader
	cache       string
}

func LoadCollections(collections []*CollectionConfig, cache string, downloadOnly bool, stats *report.Recorder) (*CollectionSet, error) {
	cs := new(CollectionSet)
	cs.Collections = make(map[string]*Reader)

	if len(collections) < 1 {
		return nil, fmt.Errorf("no collections to load!")
	}

	if err := downloadCollections(collections, cache, stats); err != nil {
		log.Println("[LoadCollections] Error fetching collections: ", err)
		return nil, err
	}

	if downloadOnly {
		return nil, nil
	}

	t := time.Now()
	for _, cfg := range collections {
		reader, err := NewReaderFromConfig(*cfg)
		if err != nil {
			return nil, err
		}

		cs.Collections[cfg.Name] = reader
	}
	if stats != nil {
		stats.TimeSince("startup.read", t)
	}

	return cs, nil
}

func downloadCollections(collections []*CollectionConfig, cache string, stats *report.Recorder) error {
	if stats != nil {
		t := time.Now()
		defer stats.TimeSince("startup.download", t)
	}
	for _, cfg := range collections {
		if cfg.LocalPath == "" {
			cfg.LocalPath = cfg.SourcePath
		}

		remote := isRemote(cfg.SourcePath)
		if remote {
			cfg.LocalPath = localCache(cfg.SourcePath, cache)
			if _, err := os.Stat(cfg.LocalPath); err == nil {
				if cfg.Debug {
					log.Printf("[FetchRemote] %s already cached: %s.", cfg.Name, cfg.LocalPath)
				}
			} else if !os.IsNotExist(err) {
				return err
			} else {
				err = fetch(cfg)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func isRemote(path string) bool {
	return strings.HasPrefix(path, "http://") || strings.HasPrefix(path, "https://")
}

func localCache(url, cache string) string {
	h := md5.Sum([]byte(url))
	name := hex.EncodeToString(h[:]) + ".hfile"
	return path.Join(cache, name)
}

func fetch(cfg *CollectionConfig) error {
	log.Printf("[FetchRemote] Fetching %s: %s -> %s.", cfg.Name, cfg.SourcePath, cfg.LocalPath)

	fp, err := os.Create(cfg.LocalPath)
	if err != nil {
		return err
	}
	defer fp.Close()

	resp, err := http.Get(cfg.SourcePath)
	if err != nil {
		return err
	}
	if resp.StatusCode >= 400 {
		buf := new(bytes.Buffer)
		buf.ReadFrom(resp.Body)
		return fmt.Errorf("HTTP error fetching (%s): %s\n", resp.Status, buf.String())
	}
	defer resp.Body.Close()

	sz := int64(0)

	if cfg.LoadMethod == CopiedToMem && resp.ContentLength > 0 {
		buf := offheapMalloc(resp.ContentLength)
		read, err := io.ReadFull(resp.Body, buf)
		if err != nil {
			return err
		}
		if read != int(resp.ContentLength) {
			log.Printf("[FetchRemote] Uh-oh: read %d bytes, but Content-Length was %d", read, resp.ContentLength)
		}
		sz = int64(read / (1024 * 1024))
		cfg.cachedContent = &buf
		go func() {
			log.Printf("[FetchRemote] Flushing %s (%dmb) to disk...\n", cfg.Name, sz)
			if wrote, err := fp.Write(buf); err != nil {
				log.Fatal("[FetchRemote] Error flushing", cfg.Name, err)
			} else if wrote != read {
				log.Printf("[FetchRemote] Read %db but wrote %db!\n", read, wrote)
			}
			log.Printf("[FetchRemote] Flushed %s (%dmb) to disk.\n", cfg.Name, sz)
		}()
	} else {
		sz, err = io.Copy(fp, resp.Body)
		sz = sz / (1024 * 1024)
		if err != nil {
			return err
		}
	}

	log.Printf("[FetchRemote] Fetched %s (%dmb).", cfg.Name, sz)
	return nil
}

func (cs *CollectionSet) ReaderFor(name string) (*Reader, error) {
	c, ok := cs.Collections[name]
	if !ok {
		return nil, fmt.Errorf("not configured with reader for collection %s", name)
	}
	return c, nil
}
