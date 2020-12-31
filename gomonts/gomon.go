package main

import (
	"fmt"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/struCoder/pidusage"
	mon "go.scope.charter.com/lib-monitor"
	"go.scope.charter.com/lib-monitor/appd"
	"go.scope.charter.com/tsdb/v2"
)

type mmetric struct {
	name string // metric name for TSDB, or "" to use field name
	d    bool   // false, for current value, true for diff with previous
}

const type_gomon = "gomon"
const type_calls = "calls"

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
func GoMoInit(a, v string, rk []string, m mon.Monitor, c tsdb.Conf) {
	initTime = time.Now()
	app = a
	ver = v
	host, _ = os.Hostname()
	tsc = tsdb.NewHttpClient(c, m)
	pid = os.Getpid()
	pids = strconv.Itoa(pid)

	go runMonitor(md)
	go runInfo(sd, rk)
	go runAppdBt(md, m)
	respMonInit()
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

func runAppdBt(d time.Duration, m mon.Monitor) {

	tags := []tsdb.Tag{{Key: "app", Value: app},
		{Key: "host", Value: host},
		{Key: "id", Value: pids},
	}

	for {
		time.Sleep(5 * time.Second) // Initial delay
		dmap := appd.BtStats()

		for metric, mval := range dmap {
			addToTsdb(type_gomon, metric, float64(mval), tags)
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
		log.Errorf(">>> Bad Value, addToTsdb failed >> TSDB Client: %+v, appName: %s", tsc, app)
		return
	}

	utime := int(time.Now().Unix())
	dp := tsdb.DataPoint{
		Metric:   "scope." + t + "." + m,
		Unixtime: utime,
		Value:    v,
	}

	dp.Tags = tags
	log.Debugf("metricToTsdb: %+v", dp)
	tsc.Put(dp)
}
