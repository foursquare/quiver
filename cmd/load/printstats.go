package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/dt/go-metrics"
	"github.com/dt/go-metrics-reporting"
)

func PrintSummary(rttName, diffRtt string, isDiffing bool) {
	du := float64(time.Millisecond)
	overall := report.GetDefault().Get(rttName + ".overall").(metrics.Timer)

	if isDiffing {
		diffOverall := report.GetDefault().Get(diffRtt + ".overall").(metrics.Timer)
		useful := []float64{0.50, 0.90, 0.99}
		ps, psDiff := overall.Percentiles(useful), diffOverall.Percentiles(useful)
		fmt.Printf("%10s\t\t%10s\n", rttName, diffRtt)
		fmt.Printf("\tp99 %6.2fms\t%6.2fms\t(%6.2fms)\n", ps[2]/du, psDiff[2]/du, (ps[2]-psDiff[2])/du)
		fmt.Printf("\tp90 %6.2fms\t%6.2fms\t(%6.2fms)\n", ps[1]/du, psDiff[1]/du, (ps[1]-psDiff[1])/du)
		fmt.Printf("\tp50 %6.2fms\t%6.2fms\t(%6.2fms)\n", ps[0]/du, psDiff[0]/du, (ps[0]-psDiff[0])/du)
		report.GetDefault().Each(func(stat string, i interface{}) {
			if strings.HasPrefix(stat, "diffs.") {
				switch m := i.(type) {
				case metrics.Meter:
					fmt.Printf("%s %d (%d total)\n", m.Rate1(), m.Count())
				default:
					fmt.Printf("%s %T %v\n", m, m)
				}
			}
		})
	} else {
		fmt.Printf("%s\t(%6.2fqps)\tp99 %6.2fms\n", rttName, overall.Rate1(), overall.Percentile(0.99)/du)
	}
	queue := report.GetDefault().Get("queue").(metrics.Gauge).Value()
	dropped := report.GetDefault().Get("dropped").(metrics.Meter)
	fmt.Printf("queue %d (dropped: %.2f)\n", queue, dropped.Rate1())
}
