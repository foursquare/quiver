package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/dt/curator.go"
	"github.com/dt/go-curator-discovery"
	"github.com/foursquare/quiver/hfile"
)

type Registrations struct {
	existing []*discovery.ServiceDiscovery

	zk curator.CuratorFramework

	sync.Mutex
}

func (r *Registrations) Connect() {
	if Settings.zk == "" {
		log.Fatal("Specified discovery path but not zk?", Settings.zk)
	}

	retryPolicy := curator.NewExponentialBackoffRetry(time.Second, 3, 15*time.Second)

	r.zk = curator.NewClient(Settings.zk, retryPolicy)

	if err := r.zk.Start(); err != nil {
		log.Fatal(err)
	}
}

func (r *Registrations) Join(hostname, base string, configs []*hfile.CollectionConfig) {
	if hostname == "localhost" {
		log.Fatal("invalid hostname for service discovery registration:", hostname)
	}

	r.Lock()
	defer r.Unlock()

	for _, i := range configs {
		sfunc := i.ShardFunction
		capacity := i.TotalPartitions

		if len(sfunc) < 1 {
			capacity = "1"
			sfunc = "_"
		}

		base := fmt.Sprintf("%s/%s/%s", i.ParentName, sfunc, capacity)

		disco := discovery.NewServiceDiscovery(r.zk, curator.JoinPath(Settings.discoveryPath, base))
		if err := disco.MaintainRegistrations(); err != nil {
			log.Fatal(err)
		}
		s := discovery.NewSimpleServiceInstance(i.Partition, hostname, Settings.port)
		disco.Register(s)
		r.existing = append(r.existing, disco)
	}
}

func (r *Registrations) Leave() {
	r.Lock()
	defer r.Unlock()
	for _, reg := range r.existing {
		reg.UnregisterAll()
	}
}

func (r *Registrations) Close() {
	r.Leave()
	r.zk.Close()
}
