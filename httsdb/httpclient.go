package httsdb

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	log "github.com/sirupsen/logrus"
	mon "go.scope.charter.com/lib-monitor"
)

// Conf has http client conf
type Conf struct {
	Host            string
	Port            int
	DefaultBuffer   int
	DefaultInterval int
	HTTPConf        HTTPConf `mapstructure:"http"`
}

// HTTPConf keeps all http related configs
type HTTPConf struct {
	DialTimeout         time.Duration `mapstructure:"dialTimeout"`
	TLSHandshakeTimeout time.Duration `mapstructure:"tlsHandShakeTimeout"`
	MaxIdleConnsPerHost int           `mapstructure:"maxIdleConnsPerHost"`
	MaxIdleConns        int           `mapstructure:"maxIdleConns"`
	IdleConnTimeout     time.Duration `mapstructure:"idleConnTimeout"`
	ClientTimeout       time.Duration `mapstructure:"clientTimeout"`
}

var (
	ErrQuery        = errors.New("Failed to run query")
	ErrResponseRead = errors.New("Failed to read response")
)

type HttpClient struct {

	//Number of datapoints to buffer before flushing
	BufferSize int

	//Number of seconds before writitng what we've buffered
	Interval int

	// For Monitoring
	bm mon.Monitor

	base_url string
	client   *http.Client
	putChan  chan DataPoint
}

func NewHttpClient(conf Conf, bMonitor mon.Monitor) *HttpClient {
	var netTransport = &http.Transport{
		Dial: (&net.Dialer{
			Timeout: conf.HTTPConf.DialTimeout,
		}).Dial,
		TLSHandshakeTimeout: conf.HTTPConf.TLSHandshakeTimeout,
		MaxIdleConnsPerHost: conf.HTTPConf.MaxIdleConnsPerHost,
		MaxIdleConns:        conf.HTTPConf.MaxIdleConns,
		IdleConnTimeout:     conf.HTTPConf.IdleConnTimeout,
	}

	t := &HttpClient{
		BufferSize: conf.DefaultBuffer,
		Interval:   conf.DefaultInterval,
		client: &http.Client{
			Transport: netTransport,
			Timeout:   conf.HTTPConf.ClientTimeout,
		},
		base_url: fmt.Sprintf("http://%s:%d/api", conf.Host, conf.Port),
		putChan:  make(chan DataPoint, 1000),
	}

	// busboy monitor
	t.bm = bMonitor

	if t.Interval != 0 && t.BufferSize != 0 {
		go t.writer()
	} else {
		log.Warningf("Not starting writer for tsdb client, if you want one please set interval and buffer")
	}

	return t
}

func (t *HttpClient) GetPlain(q QueryRequest) ([]Result, error) {

	var (
		resp *http.Response
		err  error
	)
	res := []Result{}

	endpoint := "query"
	b, err := q.ToJson()
	if err != nil {
		return res, fmt.Errorf("Failed to make query: %+v", err)
	}

	buff := bytes.NewBuffer(b)

	url := fmt.Sprintf("%s/%s", t.base_url, endpoint)
	req, err := http.NewRequest("POST", url, buff)
	if err != nil {
		e := fmt.Errorf("Failed to generate request: %+v", err)
		return res, e
	}
	req.Header.Add("Accept-Encoding", "gzip")

	if resp, err = t.client.Do(req); err != nil {
		return res, fmt.Errorf("Failed to do request: %+v", err)
	}
	defer resp.Body.Close()

	gzipReader, err := gzip.NewReader(resp.Body)
	if err != nil {
		//Got nothing back, thats fine
		if err == io.EOF {
			return res, nil
		}
		e := fmt.Errorf("Failed to read gzip: %+v", err)
		return res, e
	}

	if resp.StatusCode != 200 {

		errRes := map[string]map[string]interface{}{}
		if err = json.NewDecoder(gzipReader).Decode(&errRes); err != nil {
			e := fmt.Errorf("Failed to decode: %+v", err)
			return res, e
		}

		message := "Unknown Error"
		if o, ok := errRes["error"]; ok {
			message = o["message"].(string)
		}
		e := fmt.Errorf("ResponseCode: %d (%s)", resp.StatusCode, message)
		return res, e
	}

	if err = json.NewDecoder(gzipReader).Decode(&res); err != nil {
		e := fmt.Errorf("Failed to unmarshal: %+v", err)
		return res, e
	}

	return res, nil
}

func (t *HttpClient) Get(q QueryRequest, bt mon.Transaction) ([]Result, error) {

	var (
		resp *http.Response
		err  error
	)
	res := []Result{}

	endpoint := "query"

	url := fmt.Sprintf("%s/%s?%s", t.base_url, endpoint, q.Parameterize().Encode())
	req, err := http.NewRequest("GET", url, nil)
	log.Debugf("GET url: %v", url)
	if err != nil {
		e := fmt.Errorf("Failed to generate request: %+v", err)
		return res, e
	}
	req.Header.Add("Accept-Encoding", "gzip")

	exitcall, _ := bt.Exit("tsdb", url)
	if resp, err = t.client.Do(req); err != nil {
		exitcall.EndWithError(err)
		return res, fmt.Errorf("Failed to do request: %+v", err)
	}
	exitcall.End()
	defer resp.Body.Close()

	gzipReader, err := gzip.NewReader(resp.Body)
	if err != nil {
		//Got nothing back, thats fine
		if err == io.EOF {
			return res, nil
		}
		e := fmt.Errorf("Failed to read gzip: %+v", err)
		bt.Error(e)
		return res, e
	}

	if resp.StatusCode != 200 {

		errRes := map[string]map[string]interface{}{}
		if err = json.NewDecoder(gzipReader).Decode(&errRes); err != nil {
			e := fmt.Errorf("Failed to decode: %+v", err)
			bt.Error(e)
			return res, e
		}

		message := "Unknown Error"
		if o, ok := errRes["error"]; ok {
			message = o["message"].(string)
		}
		e := fmt.Errorf("ResponseCode: %d (%s)", resp.StatusCode, message)
		bt.Error(e)
		return res, e
	}

	if err = json.NewDecoder(gzipReader).Decode(&res); err != nil {
		e := fmt.Errorf("Failed to unmarshal: %+v", err)
		bt.Error(e)
		return res, e
	}

	return res, nil
}

func (t *HttpClient) PutMany(dps []DataPoint) error {

	var (
		buf  bytes.Buffer
		err  error
		resp *http.Response
		req  *http.Request
	)

	url := fmt.Sprintf("%s/%s", t.base_url, "put")

	gw := gzip.NewWriter(&buf)

	if err = json.NewEncoder(gw).Encode(dps); err != nil {
		return err
	}

	if err = gw.Close(); err != nil {
		return err
	}

	if req, err = http.NewRequest("POST", url, &buf); err != nil {
		return err
	}
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Content-Encoding", "gzip")

	len, start := buf.Len(), time.Now()
	if resp, err = t.client.Do(req); err != nil {
		return err
	}
	log.Debugf("%d bytes in %+v", len, time.Since(start))

	defer resp.Body.Close()

	if resp.StatusCode == 204 {
		return nil
	}

	rv := map[string]interface{}{}
	if err := json.NewDecoder(resp.Body).Decode(&rv); err != nil {
		return err
	}

	if _, ok := rv["error"]; ok {
		errmap := rv["error"].(map[string]interface{})
		return errors.New(errmap["message"].(string))
	}

	return nil
}

// Put sends to a put chan that is put with many when buffer is filled or timeout
func (t *HttpClient) Put(dp DataPoint) {
	t.putChan <- dp
}

func (t *HttpClient) writer() {

	buffer := []DataPoint{}

	ticker := time.NewTicker(time.Duration(t.Interval) * time.Second)
	for {
		select {
		case m := <-t.putChan:
			buffer = append(buffer, m)
			if len(buffer) < t.BufferSize {
				continue
			}
		case <-ticker.C:
			if len(buffer) == 0 {
				continue
			}
		}

		if err := t.PutMany(buffer); err != nil {
			log.Errorf("Failed to put many data points: %+v", err)
		}

		buffer = []DataPoint{}
	}
}

// CloseIdleConnections to close idle transport connections
func (t *HttpClient) CloseIdleConnections() {
	t.client.CloseIdleConnections()
}

//SetClientTimeout to update timeout
func (t *HttpClient) SetClientTimeout(val time.Duration) {
	t.client.Timeout = val
}
