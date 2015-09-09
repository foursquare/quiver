package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

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
	Url           string
	Ondemand      bool
}

func ConfigsFromJsonUrl(url string) ([]*hfile.CollectionConfig, error) {
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

	ret := make([]*hfile.CollectionConfig, len(specs.Collections))
	for i, spec := range specs.Collections {
		if spec.Url != "" {
			name := fmt.Sprintf("%s/%d", spec.Collection, spec.Partition)
			mlock := true
			if spec.Ondemand {
				mlock = false
			}
			ret[i] = &hfile.CollectionConfig{name, spec.Url, "", mlock, Settings.debug}
		}
	}

	log.Printf("Found %d collections in config:", len(ret))
	for _, cfg := range ret {
		if Settings.debug {
			log.Printf("\t%s (%s)", cfg.Name, cfg.SourcePath)
		} else {
			log.Printf("\t%s", cfg.Name)
		}
	}

	return ret, nil
}
