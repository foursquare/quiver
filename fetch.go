package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
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
	Url           string
	Ondemand      bool
}

func getCollectionConfig(args []string) []*hfile.CollectionConfig {
	if Settings.configJsonUrl != "" {
		if len(args) > 0 {
			log.Fatalln("Only one of command-line collection specs or json config may be used.")
		}
		return ConfigsFromJsonUrl(Settings.configJsonUrl)
	}
	return ConfigsFromCommandline(args)
}

func ConfigsFromCommandline(args []string) []*hfile.CollectionConfig {
	configs := make([]*hfile.CollectionConfig, len(args))

	for i, pair := range args {
		nameAndPath := strings.SplitN(pair, "=", 2)
		if len(nameAndPath) != 2 {
			log.Fatal("collections must be specified in the form 'name=path'")
		}

		name, sfunc, total, part := nameAndPath[0], "_", "1", "0"

		parent := name

		if details := strings.SplitN(name, "/", 4); len(details) == 4 {
			parent = details[0]
			sfunc, total, part = details[1], details[2], details[3]
			name = fmt.Sprintf("%s/%s", parent, part)
		}

		loadMethod := hfile.CopiedToMem
		if Settings.mlock {
			loadMethod = hfile.MemlockFile
		}

		configs[i] = &hfile.CollectionConfig{
			Name:            name,
			SourcePath:      nameAndPath[1],
			LocalPath:       nameAndPath[1],
			LoadMethod:      loadMethod,
			Debug:           Settings.debug,
			ParentName:      parent,
			ShardFunction:   sfunc,
			Partition:       part,
			TotalPartitions: total,
		}
	}

	return configs
}

func ConfigsFromJsonUrl(url string) []*hfile.CollectionConfig {
	if Settings.debug {
		log.Printf("[ConfigsFromJsonUrl] Fetching config from %s...\n", url)
	}
	res, err := http.Get(url)
	if err != nil {
		log.Fatal(err)
	}
	if Settings.debug {
		log.Printf("[ConfigsFromJsonUrl] Fetched. Parsing...\n")
	}
	defer res.Body.Close()

	var specs CollectionSpecList

	if err := json.NewDecoder(res.Body).Decode(&specs); err != nil {
		log.Fatal(err)
	}

	if Settings.debug {
		log.Printf("[ConfigsFromJsonUrl] Found %d collections.\n", len(specs.Collections))
	}

	ret := make([]*hfile.CollectionConfig, len(specs.Collections))
	for i, spec := range specs.Collections {
		if spec.Url != "" {
			name := fmt.Sprintf("%s/%d", spec.Collection, spec.Partition)

			loadMethod := hfile.CopiedToMem
			if Settings.mlock {
				loadMethod = hfile.MemlockFile
			}

			if spec.Ondemand {
				loadMethod = hfile.OnDisk
			}

			ret[i] = &hfile.CollectionConfig{
				Name:            name,
				SourcePath:      spec.Url,
				LocalPath:       "",
				LoadMethod:      loadMethod,
				Debug:           Settings.debug,
				ParentName:      spec.Collection,
				ShardFunction:   spec.Function,
				Partition:       fmt.Sprintf("%d", spec.Partition),
				TotalPartitions: fmt.Sprintf("%d", spec.Capacity),
			}
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
	return ret
}
