package gomonts

import (
	"fmt"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"time"

	"concheck/tsdb"

	"github.com/struCoder/pidusage"
)

type mmetric struct {
	name string // metric name for TSDB, or "" to use field name
	d    bool   // false, for current value, true for diff with previous
}

type AddFunc func(m string, v float64, mt []tsdb.Tag)

const type_gomon = "gomon"

// some consts for time being
var md = 60 * time.Second
var sd = 6 * time.Hour

var app, ver, host string
var tsc *tsdb.HttpClient
var moreMetrics map[string]mmetric
var pid int
var pids string
var initTime time.Time

// GoMoInit has to be called from main to start the process
// send an empty rk to not run "runInfo"
// a - app name
// v - version of the app
func GoMoInit(a, v string, c tsdb.Conf) AddFunc {
	initTime = time.Now()
	app = a
	ver = v
	host, _ = os.Hostname()
	tsc = tsdb.NewHttpClient(c)
	pid = os.Getpid()
	pids = strconv.Itoa(pid)

	//go runMonitor(md)
	//go runInfo(sd, rk)

	tags := []tsdb.Tag{{Key: "app", Value: app},
		{Key: "host", Value: host},
		{Key: "id", Value: pids},
	}

	return func(m string, v float64, moretags []tsdb.Tag) {
		for _, tag := range moretags {
			tags = append(tags, tag)
		}
		addToTsdb(a, m, v, tags)
	}
}

// AddGoMoMetric to add a metric from memStat into monitoring
func AddGoMoMetric(fieldName, name string, diff bool) {
	moreMetrics[fieldName] = mmetric{name: name, d: diff}
}

func getValueByField(rtm runtime.MemStats, field string) float64 {

	r := reflect.ValueOf(rtm)
	f := reflect.Indirect(r).FieldByName(field)
	return float64(f.Int())
}

func runMonitor(d time.Duration) {

	tags := []tsdb.Tag{{Key: "app", Value: app},
		{Key: "host", Value: host},
		{Key: "id", Value: pids},
	}

	var rtmprev runtime.MemStats
	for {
		<-time.After(d)

		addToTsdb(type_gomon, "goroutines", float64(runtime.NumGoroutine()), tags)

		var rtm runtime.MemStats
		runtime.ReadMemStats(&rtm) // Full mem stats
		cpu, _ := pidusage.GetStat(pid)

		//log.Infof("gomon: MemStats: %+v", rtm)
		addToTsdb(type_gomon, "memAlloc", float64(rtm.Alloc), tags)
		addToTsdb(type_gomon, "rss", cpu.Memory, tags)
		//addToTsdb("memTotalAllocKB", float64(rtm.TotalAlloc)/1024, tags)
		addToTsdb(type_gomon, "mallocs", float64(rtm.Mallocs-rtmprev.Mallocs), tags)
		addToTsdb(type_gomon, "frees", float64(rtm.Frees-rtmprev.Frees), tags)
		addToTsdb(type_gomon, "currAllocs", float64(rtm.Mallocs-rtm.Frees), tags)
		addToTsdb(type_gomon, "memSys", float64(rtm.Sys), tags)

		addToTsdb(type_gomon, "msGcPause", float64(rtm.PauseTotalNs-rtmprev.PauseTotalNs)/1000000, tags) // in NanoSec
		addToTsdb(type_gomon, "gcNum", float64(rtm.NumGC-rtmprev.NumGC), tags)
		addToTsdb(type_gomon, "percentCPU", cpu.CPU, tags)

		for f, mm := range moreMetrics {
			tname := f
			if mm.name != "" {
				tname = mm.name
			}

			fv := getValueByField(rtm, f)
			if !mm.d {
				addToTsdb(type_gomon, tname, fv, tags)
				continue
			}
			ofv := getValueByField(rtmprev, f)
			addToTsdb(type_gomon, tname, fv-ofv, tags)
		}
		rtmprev = rtm
	}
}

func runInfo(d time.Duration, rks []string) {

	tags := []tsdb.Tag{{Key: "app", Value: app},
		{Key: "version", Value: ver},
		{Key: "host", Value: host},
		{Key: "id", Value: pids},
	}

	for {
		time.Sleep(5 * time.Second) // Initial delay

		for ix, rk := range rks {
			keyn := fmt.Sprintf("rk_%d", ix+1)
			thisRkTag := append(tags, tsdb.Tag{Key: keyn, Value: rk})
			addToTsdb(type_gomon, "updays", time.Since(initTime).Hours()/24, thisRkTag)
		}
		<-time.After(d)
	}
}

// addToTsdb adds a metric to tsdb
// metric name is "scope" + t + m with a `.` in between
// t is the type of the metric, like gomon, calls, etc
// m is the metric name
func addToTsdb(t, m string, v float64, tags []tsdb.Tag) {
	if tsc == nil || app == "" {
		fmt.Printf(">>> Bad Value, addToTsdb failed >> TSDB Client: %+v, appName: %s", tsc, app)
		return
	}

	utime := int(time.Now().Unix())
	dp := tsdb.DataPoint{
		Metric:   t + "." + m,
		Unixtime: utime,
		Value:    v,
	}

	dp.Tags = tags
	fmt.Printf("metricToTsdb: %+v\n", dp)
	tsc.PutOne(dp)
}
