package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/dt/go-metrics"
	"github.com/dt/go-metrics-reporting"
)

func PrintSummary(l *Load) {
	fmt.Println(string([]byte{27}) + "[2J")
	du := float64(time.Millisecond)
	overall := report.GetDefault().Get(l.rtt + ".overall").(metrics.Timer)

	if l.diffing {
		diffOverall := report.GetDefault().Get(l.diffRtt + ".overall").(metrics.Timer)
		useful := []float64{0.50, 0.90, 0.99}
		ps, psDiff := overall.Percentiles(useful), diffOverall.Percentiles(useful)
		fmt.Printf("%10s\t\t%10s\n", l.rtt, l.diffRtt)
		fmt.Printf("\tp99 %6.2fms\t%6.2fms\t(%6.2fms)\n", ps[2]/du, psDiff[2]/du, (ps[2]-psDiff[2])/du)
		fmt.Printf("\tp90 %6.2fms\t%6.2fms\t(%6.2fms)\n", ps[1]/du, psDiff[1]/du, (ps[1]-psDiff[1])/du)
		fmt.Printf("\tp50 %6.2fms\t%6.2fms\t(%6.2fms)\n", ps[0]/du, psDiff[0]/du, (ps[0]-psDiff[0])/du)
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
	} else {
		fmt.Printf("%s\tp99 %6.2fms\n", l.rtt, overall.Percentile(0.99)/du)
	}
	queue := report.GetDefault().Get("queue").(metrics.Gauge).Value()
	dropped := report.GetDefault().Get("dropped").(metrics.Meter)
	fmt.Printf("%4.2f qps. queue %d (dropped: %.2f).\n", overall.Rate1(), queue, dropped.Rate1())
}
