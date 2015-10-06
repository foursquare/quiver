package main

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/dt/go-metrics"
	"github.com/dt/go-metrics-reporting"
)

func PrintDiffTimingLine(name string, x, y, du float64) {
	fmt.Printf("\t%s:\t%6.2fms\t%6.2fms\t%+.2fms\t%+.2f%%\n", name, x/du, y/du, (y-x)/du, (y-x)/x*100)
}

func (l *Load) PrintTiming(suffix string, du float64) {
	useful := []float64{0.50, 0.90, 0.99}

	orig := report.GetDefault().Get(l.rtt + suffix)
	if orig == nil {
		fmt.Println(l.rtt, "missing timing:", suffix)
		return
	}
	ps := orig.(metrics.Timer).Percentiles(useful)

	if l.diffing {
		diff := report.GetDefault().Get(l.diffRtt + suffix)
		if diff == nil {
			fmt.Println(l.diffRtt, "missing timing:", suffix)
			return
		}
		psDiff := diff.(metrics.Timer).Percentiles(useful)
		qps := orig.(metrics.Timer).Rate1()

		fmt.Printf("%s (%.1fqps)\n\t\t%-15s\t%-15s\n", suffix[1:], qps, l.rtt[4:], l.diffRtt[4:])
		PrintDiffTimingLine("p99", ps[2], psDiff[2], du)
		PrintDiffTimingLine("p90", ps[1], psDiff[1], du)
		PrintDiffTimingLine("p50", ps[0], psDiff[0], du)
		fmt.Println()
	} else {
		fmt.Printf("%s\tp99\t%6.2fms\tp50 %6.2f\n", l.rtt, ps[2]/du, ps[0]/du)
	}
}

func (l *Load) PrintSummary() {
	fmt.Println(string([]byte{27}) + "[2J")
	du := float64(time.Millisecond)
	overall := report.GetDefault().Get(l.rtt + ".overall")
	if overall == nil {
		fmt.Println("no timings yet.")
		return
	}
	qps := overall.(metrics.Timer).Rate1()

	l.PrintTiming(".overall", du)

	var interesting []string
	seen := make(map[string]bool)
	report.GetDefault().Each(func(stat string, i interface{}) {
		switch i.(type) {
		case metrics.Timer:
			if !strings.HasSuffix(stat, ".overall") {
				s := ""
				if strings.HasPrefix(stat, l.rtt) {
					s = strings.TrimPrefix(stat, l.rtt)
				} else if strings.HasPrefix(stat, l.diffRtt) {
					s = strings.TrimPrefix(stat, l.diffRtt)
				}
				if !seen[s] {
					seen[s] = true
					interesting = append(interesting, s)
				}
			}
		}
	})

	sort.Strings(interesting)

	for _, i := range interesting {
		l.PrintTiming(i, du)
	}

	if l.diffing {
		report.GetDefault().Each(func(stat string, i interface{}) {
			if strings.HasPrefix(stat, "diffs.") {
				switch m := i.(type) {
				case metrics.Meter:
					fmt.Printf("%s %4.2f/s (%d total)\n", stat, m.Rate1(), m.Count())
				default:
					fmt.Printf("%s %T %v\n", stat, m, m)
				}
			}
		})
	}

	queue := report.GetDefault().Get("queue").(metrics.Gauge).Value()
	dropped := report.GetDefault().Get("dropped").(metrics.Meter)
	fmt.Printf("%4.2f qps. queue %d (dropped: %.2f).\n", qps, queue, dropped.Rate1())
}
