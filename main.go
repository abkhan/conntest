/*
 * ABK-Services
 * Connection Check Script
 */

package main

import (
	"concheck/conf"
	"concheck/gomonts"
	"errors"
	"flag"
	"fmt"
	"net"
	"time"

	config "concheck/conf"
	"concheck/tsdb"

	"github.com/go-ping/ping"
	fastping "github.com/tatsushid/go-fastping"
)

type sconf struct {
	App  appConfig `maspstructure:"app"`
	Tsdb tsdb.Conf `mapstructure:"tsdb"`
}
type appConfig struct {
	Name string `mapstructure:"name"`
}

var (
	startTime   = time.Now().String()
	destination string
	pingCount   = 9
	delayCount  = 4
)

func main() {
	flag.StringVar(&destination, "d", "4.2.2.2", "address to ping")
	flag.Parse()

	fmt.Printf("StartTime: %s\n", startTime)
	fmt.Printf("Destination: %s\n", destination)

	c := sconf{}
	conf.Load(&c)

	if err := config.ValidateConf(c); err != nil {
		fmt.Println(err)
	}
	fmt.Printf("Config: %+v\n", c)

	// *** ping loop ***
	var trtt time.Duration
	errorc := 0

	fmt.Println("Ping Loop Start: " + time.Now().String())
	for c := 0; c < pingCount-1; c++ {

		if t, e := doPing(destination); e != nil {
			fmt.Println("doPing error: " + e.Error())
			errorc++
		} else {
			//fmt.Printf("RTT: %+v\n", t)
			trtt += t.AvgRtt
		}

		time.Sleep(time.Duration(delayCount) * time.Second)
	}
	if t, e := doPing(destination); e != nil {
		fmt.Println("last doPing error: " + e.Error())
		errorc++
	} else {
		//fmt.Printf("last RTT: %+v\n", t)
		trtt += t.AvgRtt
	}

	var avgDur time.Duration
	var goodping int = 0
	if errorc != pingCount {
		goodping = pingCount - errorc
		avgDur = time.Duration(int64(trtt) / (int64(goodping)))
	}
	avgDurS := avgDur.String()
	fmt.Printf("Ping Loop [%s]: %s\n", avgDurS, time.Now().String())

	// *** write to tsdb ***
	// Write the two values to tsdb alongwith hostname
	addfunc := gomonts.GoMoInit(c.App.Name, "0.0.2", c.Tsdb)
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
		panic(err)
	}
	pinger.Count = 3
	err = pinger.Run() // Blocks until finished.
	if err != nil {
		return &ping.Statistics{}, err
	}
	stats := pinger.Statistics() // get send/receive/rtt stats
	return stats, nil
}
