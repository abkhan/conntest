package tsdb

import "strings"

type TsdbQueryRequest struct {
	Start     string      `json:"start"`
	End       string      `json:"end"`
	Queries   []TsdbQuery `json:"queries"`
	ShowQuery bool        `json:"showQuery"`
}

type TsdbQuery struct {
	Aggregator string            `json:"aggregator"`
	Metric     string            `json:"metric"`
	Rate       bool              `json:"rate"`
	RateOpts   TsdbRateOpts      `json:"rateOpts,omitempty"`
	Downsample string            `json:"downsample,omitempty"`
	Tags       map[string]string `json:"tags"`
	Filters    []Filter          `json:"filters"`
}

type TsdbRateOpts struct {
	Counter    bool `json:"counter"`
	CounterMax int  `json:"counterMax"`
	ResetVal   int  `json:"resetValue"`
	DropResets bool `json:"dropResets"`
}

func CreateTsdbQueryRequest(q QueryRequest) TsdbQueryRequest {
	tq := TsdbQueryRequest{}
	tq.Start = q.Window.GetStart()

	if end, ok := q.Window.GetEnd(); ok {
		tq.End = end
	}
	tq.Queries = ConvertQueriesToTsdb(q.Queries)
	return tq
}

func ConvertQueriesToTsdb(q []Query) []TsdbQuery {
	tqs := []TsdbQuery{}
	for _, oq := range q {
		tagmap := map[string]string{}
		for tk, tvs := range oq.Tags {
			tagmap[tk] = strings.Join(tvs, "|")
		}
		tq := TsdbQuery{
			Aggregator: oq.AggFunc,
			Metric:     oq.Metric,
			Rate:       oq.Rate,
			Tags:       tagmap,
			Filters:    oq.Filters,
		}

		if !oq.Downsampler.RelativeTime.IsNull() {
			tq.Downsample = oq.Downsampler.String()
		}
		tqs = append(tqs, tq)
	}
	return tqs
}
