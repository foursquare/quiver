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
type Registration struct {
	base, name string
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

func ConfigsFromJsonUrl(url string) ([]*hfile.CollectionConfig, []Registration, error) {
	if Settings.debug {
		log.Printf("[ConfigsFromJsonUrl] Fetching config from %s...\n", url)
	}
	res, err := http.Get(url)
	if err != nil {
		return nil, nil, err
	}
	if Settings.debug {
		log.Printf("[ConfigsFromJsonUrl] Fetched. Parsing...\n")
	}
	defer res.Body.Close()

	var specs CollectionSpecList

	if err := json.NewDecoder(res.Body).Decode(&specs); err != nil {
		return nil, nil, err
	}

	if Settings.debug {
		log.Printf("[ConfigsFromJsonUrl] Found %d collections.\n", len(specs.Collections))
	}

	reg := make([]Registration, len(specs.Collections))
	ret := make([]*hfile.CollectionConfig, len(specs.Collections))
	for i, spec := range specs.Collections {
		if spec.Url != "" {
			name := fmt.Sprintf("%s/%d", spec.Collection, spec.Partition)

			mlock := true
			if spec.Ondemand {
				mlock = false
			}
			ret[i] = &hfile.CollectionConfig{name, spec.Url, "", mlock, Settings.debug}

			capacity := spec.Capacity
			sfunc := spec.Function
			if len(sfunc) < 1 {
				capacity = 1
				sfunc = "_"
			}

			base := fmt.Sprintf("%s/%s/%d", spec.Collection, sfunc, capacity)

			reg[i] = Registration{base: base, name: fmt.Sprintf("%d", spec.Partition)}
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

	return ret, reg, nil
}
