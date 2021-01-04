package tsdb

type Result struct {
	Metric        string            `json:"metric"`
	Dps           map[int64]float64 `json:"dps"`
	Tags          map[string]string `json:"tags"`
	AggregateTags []string          `json:"aggregateTags"`
	Query         ResultQuery       `json:"query,omitempty"`
}

type ResultQuery struct {
	Aggregator string            `json:"aggregator"`
	Metric     string            `json:"metric"`
	Tsuids     []string          `json:"tsuids"`
	Downsample string            `json:"downsample"`
	Rate       bool              `json:"rate"`
	Filters    []Filter          `json:"filters"`
	Tags       map[string]string `json:"tags"`
}
