package tsdb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
)

type RelativeTime struct {
	Count int    `json:"count"`
	Units string `json:"units"`
}

func (r RelativeTime) IsNull() bool {
	return r.Count == 0
}
func (r RelativeTime) String() string {
	return fmt.Sprintf("%d%s-ago", r.Count, r.Units)
}

type Window struct {
	AbsoluteStart int64        `json:"absoluteStart,string"`
	AbsoluteEnd   int64        `json:"absoluteEnd,string,omitempty"`
	RelativeStart RelativeTime `json:"relativeStart,omitempty"`
	RelativeEnd   RelativeTime `json:"relativeEnd,omitempty"`
}

func (w Window) GetStart() string {
	if w.RelativeStart.IsNull() {
		return fmt.Sprintf("%d", w.AbsoluteStart)
	}
	return w.RelativeStart.String()
}

func (w Window) GetEnd() (string, bool) {
	if w.RelativeEnd.IsNull() {
		if w.AbsoluteEnd == 0 {
			return "", false
		}
		return fmt.Sprintf("%d", w.AbsoluteEnd), true
	}
	return w.RelativeEnd.String(), true
}

type QueryRequest struct {
	Window       Window  `json:"window"`
	Queries      []Query `json:"queries"`
	ShowQuery    bool    `json:"showQuery"`
	MSResolution bool    `json:"msResolution"`
	/*
		MSResolution - bool
		Whether or not to output data point timestamps in milliseconds or seconds.

		true = output data in miliseconds
		false = output data in seconds

		If this flag is not provided, It will default to false and use seconds.

		WARNING - Potential to overwrite "false" with unix timestamps:

		Important: MSResolution when set to false, can be overwritten by supplying 13 digit
		Unix(POSIX) timestamps for start and end time.

		Queries using Unix timestamps can also support millisecond precision by using unix
		milsecond timestamp format above.

		Unix times are defined as the number of seconds that have elapsed since January 1st, 1970
		at 00:00:00 UTC time.

		Example:

		1364410924, representing ISO 8601:2013-03-27T19:02:04Z

		Adding on 3 digits to timestamp = milisecond precision:

		For example:

		start time of 1364410924000 (13 digits = MS)

		&&

		end time of 1364410924250 (13 digits = MS)

		will return data within a 250 millisecond window.  Millisecond timestamps may also be
		supplied with a period separating the seconds from the milliseconds as in 1364410924.250.
		Any integers with 13 (or 14) characters will be treated as a millisecond timestamp.
		Anything 10 characters or less represent seconds. Milliseconds may only be supplied
		with 3 digit precision.


	*/
}

func (q QueryRequest) ToJson() ([]byte, error) {
	return json.Marshal(CreateTsdbQueryRequest(q))
}

func (q *QueryRequest) Parameterize() url.Values {
	v := url.Values{}

	v.Set("start", q.Window.GetStart())
	if end, ok := q.Window.GetEnd(); ok {
		v.Set("end", end)
	}

	// To extract data with millisecond resolution,
	// use the /api/query endpoint and specify
	// 'ms' for GET query string key, 'msResolution' is POST body key
	// If exists, is true, regardless of [query string key] value
	// default: absent
	if q.MSResolution {
		v.Set("ms", "")
	}

	if q.ShowQuery {
		v.Set("show_query", fmt.Sprintf("%t", q.ShowQuery))
	}

	for _, qry := range q.Queries {
		qry.Parameterize(&v)
	}
	return v

}

type Query struct {
	Metric      string              `json:"metric"`
	AggFunc     string              `json:"aggregationFunction"`
	Rate        bool                `json:"rate"`
	RateOpts    RateOpts            `json:"rateOptions"`
	Tags        map[string][]string `json:"tags"`
	Filters     []Filter            `json:"filters"`
	Downsampler Downsampler         `json:"downsampler"`
}

type Filter struct {
	Type    string `json:"type"`
	Tagk    string `json:"tagk"`
	Filter  string `json:"filter"`
	GroupBy bool   `json:"groupBy"`
}

type RateOpts struct {
	Counter    bool `json:"counter"`
	CounterMax int  `json:"counterMax"`
	ResetVal   int  `json:"resetValue"`
}

type Downsampler struct {
	RelativeTime RelativeTime `json:"rollup"`
	AggFunc      string       `json:"aggregationFunction"`
}

func (d Downsampler) String() string {
	return fmt.Sprintf("%d%s-%s", d.RelativeTime.Count, d.RelativeTime.Units, d.AggFunc)
}

func (q *Query) Parameterize(v *url.Values) {
	m := []string{}
	if q.AggFunc != "" {
		m = append(m, q.AggFunc)
	}
	if !q.Downsampler.RelativeTime.IsNull() {
		m = append(m, q.Downsampler.String())
	}
	if q.Rate {
		buf := bytes.Buffer{}
		buf.WriteString("rate")
		if q.RateOpts != (RateOpts{}) {
			buf.WriteString("{")
			if q.RateOpts.CounterMax != 0 || q.RateOpts.ResetVal != 0 {
				buf.WriteString("counter,")
				if q.RateOpts.CounterMax != 0 {
					buf.WriteString(fmt.Sprint(q.RateOpts.CounterMax))
				}
				if q.RateOpts.ResetVal != 0 {
					// For default counter max w/resetVal, use 2 commas, ie "counter,,1234"
					buf.WriteString(fmt.Sprint(",", q.RateOpts.ResetVal))
				}
			} else {
				buf.WriteString("dropcounter")
			}
			buf.WriteString("}")
		}
		m = append(m, buf.String())
	}

	tags := []string{}
	for tag, vals := range q.Tags {
		tags = append(tags, fmt.Sprintf("%s=%s", tag, strings.Join(vals, "|")))
	}
	m = append(m, fmt.Sprintf("%s{%s}",
		q.Metric, strings.Join(tags, ",")))
	v.Add("m", strings.Join(m, ":"))
}
