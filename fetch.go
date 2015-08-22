package main

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path"
	"strings"

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

	fetched := make([]hfile.CollectionConfig, len(unfetched))
	for i, cfg := range unfetched {
		if isRemote, remote := IsRemote(cfg.Path); isRemote {

			if Settings.debug {
				log.Printf("[FetchCollections] %s (%s) is remote path on (%s)", cfg.Name, cfg.Path, remote)
			}

			if local, err := FetchRemote(cfg.Name, remote); err != nil {
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

func IsRemote(p string) (bool, string) {
	for prefix, format := range Settings.remotePrefixes.prefixes {
		if strings.HasPrefix(p, prefix) {
			trimmed := strings.TrimPrefix(p, prefix)
			full := fmt.Sprintf(format, trimmed)
			if Settings.debug {
				log.Printf("[IsRmote] path %s is remote: %s", trimmed, full)
			}
			return true, full
		}
	}
	return false, p
}

func FetchRemote(name, remote string) (string, error) {
	h := md5.Sum([]byte(remote))

	base := hex.EncodeToString(h[:]) + ".hfile"

	local := path.Join(Settings.cachePath, base)

	if _, err := os.Stat(local); err == nil {
		if Settings.debug {
			log.Printf("[FetchRemote] %s (%s) already exists at %s.", name, remote, local)
		}
		return local, nil
	} else if !os.IsNotExist(err) {
		if Settings.debug {
			log.Printf("[FetchRemote] %s Error checking local file %s: %v.", name, local, err)
		}
		return "", err
	}

	log.Printf("[FetchRemote] Fetching %s: %s -> %s.", name, remote, local)
	fp, err := os.Create(local)

	if err != nil {
		return "", err
	}
	defer fp.Close()

	resp, err := http.Get(remote)

	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	_, err = io.Copy(fp, resp.Body)
	if err != nil {
		return "", err
	}

	if Settings.debug {
		log.Printf("[FetchRemote] Fetched %s.", name)
	}
	return local, nil
}
