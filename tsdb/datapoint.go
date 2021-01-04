package tsdb

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

type Tag struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func NewTag(k, v string) (Tag, error) {
	if strings.Contains(k, " ") {
		return Tag{}, errors.New("Tag key cannot contain space")
	}
	if strings.Contains(v, " ") {
		return Tag{}, errors.New("Tag value cannot contain space")
	}
	return Tag{k, v}, nil
}

func MakeTags(tags map[string]string) ([]Tag, error) {
	t := []Tag{}
	for k, v := range tags {
		newTag, err := NewTag(k, v)
		if err != nil {
			return t, err
		}
		t = append(t, newTag)
	}
	return t, nil
}

func (t Tag) String() string {
	return t.Key + "=" + t.Value
}

type Tags []Tag

func (t Tags) MarshalJSON() ([]byte, error) {
	m := map[string]string{}
	for _, tag := range t {
		m[tag.Key] = tag.Value
	}
	return json.Marshal(m)
}

type DataPoint struct {
	Metric   string  `json:"metric" `
	Unixtime int     `json:"timestamp"`
	Value    float64 `json:"value"`
	Tags     Tags    `json:"tags"`
}

func NewDataPoint(metric string, unixtime int, value float64, tags []Tag) DataPoint {
	return DataPoint{metric, unixtime, value, tags}
}

func (d DataPoint) String() string {
	s := fmt.Sprintf("%s %d %f", d.Metric, d.Unixtime, d.Value)
	for _, t := range d.Tags {
		s += " " + t.String()
	}
	return s
}

func (d DataPoint) hashKey() (string, bool) {
	for _, tag := range d.Tags {
		if tag.Key == "hashKey" {
			return tag.Value, true
		}
	}
	return "", false
}
