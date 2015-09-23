package main

import (
	"fmt"
	"log"
	"time"

	"github.com/dt/curator.go"
	"github.com/dt/go-curator-discovery"
	"github.com/foursquare/gohfile"
)

func RegisterInDiscovery(hostname, base string, configs []*hfile.CollectionConfig) curator.CuratorFramework {
	if Settings.zk == "" {
		log.Fatal("Specified discovery path but not zk?", Settings.zk)
	}
	if hostname == "localhost" {
		log.Fatal("invalid hostname for service discovery registration:", hostname)
	}

	retryPolicy := curator.NewExponentialBackoffRetry(time.Second, 3, 15*time.Second)

	zk := curator.NewClient(Settings.zk, retryPolicy)
	if err := zk.Start(); err != nil {
		log.Fatal(err)
	}

	for _, i := range configs {
		sfunc := i.ShardFunction
		capacity := i.TotalPartitions

		if len(sfunc) < 1 {
			capacity = "1"
			sfunc = "_"
		}

		base := fmt.Sprintf("%s/%s/%s", i.ParentName, sfunc, capacity)

		disco := discovery.NewServiceDiscovery(zk, curator.JoinPath(Settings.discoveryPath, base))
		if err := disco.MaintainRegistrations(); err != nil {
			log.Fatal(err)
		}
		s := discovery.NewSimpleServiceInstance(i.Partition, hostname, Settings.port)
		disco.Register(s)
	}

	return zk
}
