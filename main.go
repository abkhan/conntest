/*
 * ABK-Services
 * Connection Check Script
 */

package main

import (
	"flag"
	"time"

	omon "abkhan.dynu.net/abkhan/lib-monitor"
)

var (
	vTag      = "not-set"
	vBuild    = "not-set"
	startTime = time.Now().String()

	om omon.Monitor
)

//
// Sevice Entry point
//
func main() {
	flag.Parse()

	sc := serviceConfig()

}
