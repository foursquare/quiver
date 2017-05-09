// Copyright (C) 2015 Foursquare Labs Inc.

package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/curator-go/curator"
	"github.com/foursquare/fsgo/net/discovery"
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

	r.zk = curator.NewClientTimeout(Settings.zk, 15*time.Second, 15*time.Second, retryPolicy)

	if err := r.zk.Start(); err != nil {
		log.Fatal(err)
	} else if err := r.zk.ZookeeperClient().BlockUntilConnectedOrTimedOut(); err != nil {
		log.Fatal(err)
	} else {
		log.Println("Connected to zookeeper: ", r.zk.ZookeeperClient().Connected())
	}
}

func (r *Registrations) Join(hostname, base string, configs []*hfile.CollectionConfig, wait time.Duration) {
	if hostname == "localhost" {
		log.Fatal("invalid hostname for service discovery registration:", hostname)
	}

	log.Println("Waiting to join service discovery", wait)
	time.Sleep(wait)
	log.Println("Joining service discovery...")

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
		r.existing = append(r.existing, disco)

		s := discovery.NewSimpleServiceInstance(i.Partition, hostname, Settings.port)
		disco.Register(s)

		if Settings.rpcPort > 0 {
			raw := discovery.NewSimpleServiceInstance(fmt.Sprintf("%st", i.Partition), hostname, Settings.rpcPort)
			disco.Register(raw)
		}
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
