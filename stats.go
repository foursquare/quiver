package main

import (
	"log"
	"net"
	"os"
	"time"

	"github.com/dt/go-metrics-graphite"
	"github.com/rcrowley/go-metrics"
)

var Stats metrics.Registry

func SetupStats(sendToConsole bool, sendToGraphite, graphitePrefix string) {
	r := metrics.NewRegistry()

	if sendToGraphite != "" {
		log.Println("Stats reporting to graphite: ", sendToGraphite)
		addr, _ := net.ResolveTCPAddr("tcp", sendToGraphite)

		cfg := graphite.GraphiteConfig{
			Addr:          addr,
			Registry:      r,
			FlushInterval: 15 * time.Second,
			DurationUnit:  time.Millisecond,
			Prefix:        graphitePrefix,
			Percentiles:   []float64{0.5, 0.75, 0.9, 0.95, 0.99, 0.999},
		}

		go graphite.GraphiteWithConfig(cfg)
	}

	if sendToConsole {
		log.Println("Stats reporting enabled...")
		go metrics.Log(r, time.Minute, log.New(os.Stderr, "metrics: ", log.Lmicroseconds))
	}

	Stats = r
}
