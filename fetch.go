package main

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path"
	"strings"

	"github.com/colinmarc/hdfs"
	"github.com/foursquare/gohfile"
)

type CollectionSpecList struct {
	Collections []SingleCollectionSpec
}

type SingleCollectionSpec struct {
	Capacity      int
	Collection    string
	Function      string
	LockNamespace string
	Partition     int
	Path          string
}

func LoadFromUrl(url string) ([]hfile.CollectionConfig, error) {
	configs, err := ConfigsFromJsonUrl(url)
	if err != nil {
		return nil, err
	}
	log.Printf("Found %d collections in config:", len(configs))
	for _, cfg := range configs {
		if Settings.debug {
			log.Printf("\t%s (%s)", cfg.Name, cfg.Path)
		} else {
			log.Printf("\t%s", cfg.Name)
		}
	}
	return FetchCollections(configs)
}

func ConfigsFromJsonUrl(url string) ([]hfile.CollectionConfig, error) {
	if Settings.debug {
		log.Printf("[ConfigsFromJsonUrl] Fetching config from %s...\n", url)
	}
	res, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	if Settings.debug {
		log.Printf("[ConfigsFromJsonUrl] Fetched. Parsing...\n")
	}
	defer res.Body.Close()

	var specs CollectionSpecList

	if err := json.NewDecoder(res.Body).Decode(&specs); err != nil {
		return nil, err
	}

	if Settings.debug {
		log.Printf("[ConfigsFromJsonUrl] Found %d collections.\n", len(specs.Collections))
	}

	ret := make([]hfile.CollectionConfig, len(specs.Collections))
	for i, spec := range specs.Collections {
		name := fmt.Sprintf("%s/%d", spec.Collection, spec.Partition)
		ret[i] = hfile.CollectionConfig{name, spec.Path, false}
	}
	return ret, nil
}

func FetchCollections(unfetched []hfile.CollectionConfig) ([]hfile.CollectionConfig, error) {
	if Settings.debug {
		log.Printf("[FetchCollections] Checking for non-local collections...")
	}

	var client *hdfs.Client

	fetched := make([]hfile.CollectionConfig, len(unfetched))
	for i, cfg := range unfetched {
		if download, trimmed := IsHdfs(cfg.Path); download {
			if Settings.debug {
				log.Printf("[FetchCollections] %s (%s) is an HDFS path (%s)", cfg.Name, cfg.Path, trimmed)
			}

			if client == nil && Settings.hdfsAddress != "" {
				if Settings.debug {
					log.Printf("[FetchCollections] Connecting to HDFS %s", Settings.hdfsAddress)
				}
				if c, err := hdfs.New(Settings.hdfsAddress); err != nil {
					return nil, err
				} else {
					client = c
				}
			}

			if local, err := FetchFromHdfs(client, cfg.Name, trimmed); err != nil {
				return nil, err
			} else {
				cfg.Path = local
			}
		} else if Settings.debug {
			log.Printf("[FetchCollections] %s (%s) is local path.", cfg.Name, cfg.Path)
		}
		fetched[i] = cfg
	}
	return fetched, nil
}

func IsHdfs(p string) (bool, string) {
	if len(Settings.hdfsPathPrefix) > 1 && strings.HasPrefix(p, Settings.hdfsPathPrefix) {
		if Settings.debug {
			log.Printf("[IsHdfs] Trimming %s off of %s", Settings.hdfsPathPrefix, p)
		}
		return true, strings.TrimPrefix(p, Settings.hdfsPathPrefix)
	}
	return false, p
}

func FetchFromHdfs(client *hdfs.Client, name, remote string) (string, error) {
	h := md5.Sum([]byte(remote))
	base := hex.EncodeToString(h[:]) + ".hfile"
	local := path.Join(Settings.hdfsCachePath, base)

	if _, err := os.Stat(local); err == nil {
		if Settings.debug {
			log.Printf("[FetchFromHdfs] %s (%s) already exists at %s.", name, remote, local)
		}
		return local, nil
	} else if !os.IsNotExist(err) {
		if Settings.debug {
			log.Printf("[FetchFromHdfs] %s Error checking local file %s: %v.", name, local, err)
		}
		return "", err
	}

	if client != nil {
		log.Printf("[FetchFromHdfs] Fetching %s from hdfs (using namenode %s)...\n\t%s -> %s.", name, Settings.hdfsAddress, remote, local)
		err := client.CopyToLocal(remote, local)
		if err != nil {
			return "", err
		}
	} else {
		log.Printf("[FetchFromHdfs] Running hadoop fs -copyToLocal...")
		cmd := exec.Command("hadoop", "fs", "-copyToLocal", remote, local)
		output, err := cmd.CombinedOutput()
		if err != nil {
			log.Printf("[FetchFromHdfs] Error shelling out to copyToLocal: %v:\n%s", err, output)
			return "", err
		}
	}
	if Settings.debug {
		log.Printf("[FetchFromHdfs] Fetched %s.", name)
	}
	return local, nil
}
