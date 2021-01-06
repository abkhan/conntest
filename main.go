/*
 * ABK-Services
 * Connection Check Script
 */

package main

import (
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/abkhan/gomonts"

	tsdb "github.com/abkhan/opentsdb-httpclient"

	"github.com/abkhan/config"
	"github.com/go-ping/ping"
	fastping "github.com/tatsushid/go-fastping"
)

type sconf struct {
	App  appConfig `maspstructure:"app"`
	Tsdb tsdb.Conf `mapstructure:"tsdb"`
}
type appConfig struct {
	Name         string        `mapstructure:"name"`
	PingList     string        `mapstructure:"pinglist"`  // comma separated ping list
	DelayBetween time.Duration `mapstructure:"delaybet"`  // delay in between pings
	PingCount    int           `mapstructure:"pingcount"` // do this many pings
	Save         bool          `mapstructure:"save"`      // save in tsdb or not
}

const (
	defaultCount    = 9
	defaultDelaySec = 4
	defaultPingList = "4.2.2.2,4.2.2.3,google.com"
	defaultSave     = true
	defaultName     = "wconn"
)

var (
	startTime   = time.Now().String()
	destination string
	count       = 0
	delaySec    = 0
	name        string
)

func main() {
	flag.StringVar(&name, "n", "", "default app name")
	flag.StringVar(&destination, "d", "", "addresses to ping")
	flag.IntVar(&count, "c", 0, "count of pings")
	flag.IntVar(&delaySec, "ds", 0, "delay between pings")
	flag.Parse()

	fmt.Printf("StartTime: %s\n", startTime)
	fmt.Printf("Destination: %s\n", destination)

	c := sconf{}
	if e := config.Load(&c); e != nil {
		// confile is not loaded, use defalts and more
		c.App.Name = defaultName
		c.App.PingCount = defaultCount
		c.App.PingList = defaultPingList
		c.App.DelayBetween = time.Duration(defaultDelaySec) * time.Second
	}
	if count != 0 {
		c.App.PingCount = count
	}
	if delaySec != 0 {
		c.App.DelayBetween = time.Duration(delaySec) * time.Second
	}
	if destination != "" {
		c.App.PingList = destination
	}
	if name != "" {
		c.App.Name = name
	}

	if err := config.ValidateConf(c); err != nil {
		fmt.Println(err)
	}
	fmt.Printf("Config: %+v\n", c)

	// *** ping loop ***
	var trtt time.Duration
	errorc := 0
	pingIP := strings.Split(c.App.PingList, ",")
	ipcount := len(pingIP)
	if ipcount < 1 {
		fmt.Printf("no destination\n")
		os.Exit(1)
	}

	fmt.Println(">>> Ping Loop Start: " + time.Now().String())
	for x := 0; x < c.App.PingCount-1; x++ {

		ipx := x % ipcount
		pingdest := pingIP[ipx]
		if t, e := doPing(pingdest); e != nil {
			fmt.Println("doPing error: " + e.Error())
			errorc++
		} else {
			//fmt.Printf("RTT: %+v\n", t)
			trtt += t.AvgRtt
		}

		time.Sleep(c.App.DelayBetween)
	}

	pingdest := pingIP[len(pingIP)-1]
	if t, e := doPing(pingdest); e != nil {
		fmt.Println("last doPing error: " + e.Error())
		errorc++
	} else {
		//fmt.Printf("last RTT: %+v\n", t)
		trtt += t.AvgRtt
	}

	var avgDur time.Duration
	var goodping int = 0
	if errorc != c.App.PingCount {
		goodping = c.App.PingCount - errorc
		avgDur = time.Duration(int64(trtt) / (int64(goodping)))
	}
	avgDurS := avgDur.String()
	fmt.Printf(">>> End Ping Loop [%s]: %s\n", avgDurS, time.Now().String())

	// *********************
	// *** write to tsdb ***
	// *********************

	// Write the two values to tsdb alongwith hostname
	addfunc := gomonts.GoMoInit(c.App.Name, "0.1.0", c.Tsdb)
	//tags := []tsdb.Tag{{Key: "rtt", Value: avgDurS}}
	tags := []tsdb.Tag{{Key: "failed", Value: fmt.Sprintf("%d", errorc)}}
	addfunc("ping", float64(int64(avgDur)/1000000), tags)
}

// ********************************************************
// This code does one ping and returns duration or error
// ********************************************************
func doPingFastPing(d string) (time.Duration, error) {
	p := fastping.NewPinger()
	p.Network("udp")
	p.Debug = true
	p.MaxRTT = time.Second
	idleCalld := false
	recvCalld := false

	ra, err := net.ResolveIPAddr("ip4:icmp", d)
	if err != nil {
		return 0, err
	}
	p.AddIPAddr(ra)

	rt := time.Duration(0)
	p.OnRecv = func(addr *net.IPAddr, rtt time.Duration) {
		recvCalld = true
		fmt.Printf("Rec called: %+v, %+v\n", addr, rtt)
		rt = rtt
	}
	p.OnIdle = func() {
		if !recvCalld {
			idleCalld = true
		}
	}
	err = p.Run()
	if idleCalld {
		return rt, errors.New("maxRTT reached")
	}
	if !idleCalld && !recvCalld {
		return rt, errors.New("no callback called")
	}
	return rt, err
}

func doPing(d string) (*ping.Statistics, error) {
	pinger, err := ping.NewPinger(d)
	if err != nil {
		return &ping.Statistics{}, err
	}
	pinger.Count = 3
	err = pinger.Run() // Blocks until finished.
	if err != nil {
		return &ping.Statistics{}, err
	}
	stats := pinger.Statistics() // get send/receive/rtt stats
	return stats, nil
}
