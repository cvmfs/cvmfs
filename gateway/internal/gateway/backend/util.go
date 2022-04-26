package backend

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	gw "github.com/cvmfs/gateway/internal/gateway"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

var kafkaClient *kgo.Client

func logAction(ctx context.Context, actionName string, outcome *string, t0 time.Time) {
	gw.LogC(ctx, "actions", gw.LogInfo).
		Str("action", actionName).
		Str("outcome", *outcome).
		Dur("action_dt", time.Since(t0)).
		Msg("action complete")
}

type invalidateResult struct {
	err  error
	host string
}

// Does a proxied GET of the manifest URL with "Cache-Control: no-cache" to force proxy to invalide any cached copy
// checks the returned manifest to verify that the revision is up to date
// warms the cache with the new root hash object

func InvalidateHTTPCaches(ctx context.Context, repo *RepositoryConfig, revision uint64, newRootHash string) {
	if repo.HTTPProxy.ProxyLookup == "" {
		return
	} //nothing to do

	// get a list of datamovers from the genders API
	resp, err := http.Get(repo.HTTPProxy.ProxyLookup)
	if err != nil {
		gw.LogC(ctx, "actions", gw.LogError).Err(err).Msg("Proxy lookup failed")
		return
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		gw.LogC(ctx, "actions", gw.LogError).Err(err).Msg("Proxy lookup failed reading body of reply")
		return
	}

	var buf []string
	if err = json.Unmarshal(body, &buf); err != nil {
		gw.LogC(ctx, "actions", gw.LogError).Err(err).Msg("Proxy lookup failed parsing JSON in reply")
		return
	}

	// for each datamover start a worker that warms and invalidates cache objects

	results := make(chan invalidateResult, len(buf))
	for _, p := range buf {
		go invalidateWorker(ctx, p, repo.HTTPProxy, revision, results, newRootHash)
	}
	for range buf {
		success := <-results
		if success.err != nil {
			gw.LogC(ctx, "actions", gw.LogInfo).Err(success.err).Str("proxy", success.host).Msg("Failed to invalidate manifest on proxy")
		} else {
			gw.LogC(ctx, "actions", gw.LogInfo).Str("proxy", success.host).Msg("Successfully invalidated manifest on proxy")
		}
	}
}

func invalidateWorker(ctx context.Context, dm string, proxyinfo HTTPProxy, revision uint64, success chan<- invalidateResult, newRootHash string) {
	// compose the proxy URL, including basic auth credentials

	proxy_url := fmt.Sprintf("http://%s:%s@%s:6081", proxyinfo.Username, proxyinfo.Password, dm)

	parsed_url, err := url.Parse(proxy_url)

	if err != nil {
		gw.LogC(ctx, "actions", gw.LogError).Err(err).Msgf("url.Parse of %s failed", proxy_url)
		success <- invalidateResult{err: err, host: dm}
		return
	}
	c := &http.Client{
		Transport: &http.Transport{Proxy: http.ProxyURL(parsed_url)},
		Timeout:   2 * time.Second,
	}

	//  Get the new root hash object to warm the cache

	req, err := http.NewRequest("GET", fmt.Sprintf("%s/data/%s/%s", proxyinfo.URL, newRootHash[:2], newRootHash[2:]), nil)

	if err != nil {
		gw.LogC(ctx, "actions", gw.LogError).Err(err).Msg("Failed to create HTTP request")
		success <- invalidateResult{err: err, host: dm}
		return
	}

	resp, err := c.Do(req)
	if err == nil {
		defer resp.Body.Close()
	}
	if err != nil || resp.StatusCode != 200 {
		// not a fatal error - log continue on to invalidate manifest
		gw.LogC(ctx, "actions", gw.LogError).Err(err).Msg("Failed to retrieve new root hash object from proxy")
	}

	// now invalidate the manifest object
	req, err = http.NewRequest("GET", fmt.Sprintf("%s/.cvmfspublished", proxyinfo.URL), nil)

	if err != nil {
		gw.LogC(ctx, "actions", gw.LogError).Err(err).Msg("Failed to create HTTP request")
		success <- invalidateResult{err: err, host: dm}
		return
	}

	req.Header = http.Header{"Cache-Control": []string{"no-cache"}}
	resp, err = c.Do(req)
	if err != nil {
		success <- invalidateResult{err: err, host: dm}
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		success <- invalidateResult{err: fmt.Errorf("HTTP Status Code %d", resp.StatusCode), host: dm}
		return
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		success <- invalidateResult{err: err, host: dm}
		return
	}

	// look in the manifest for the revision number
	// see https://cvmfs.readthedocs.io/en/stable/cpt-details.html#repository-manifest-cvmfspublished

	needle := fmt.Sprintf("\nS%v\n", revision)
	if !bytes.Contains(body, []byte(needle)) {
		success <- invalidateResult{err: fmt.Errorf("Returned manifest does not contain expected revision %d", revision), host: dm}
		return
	}

	success <- invalidateResult{err: nil, host: dm}
}

type kafkaMsg struct {
	Repository string `json:"repository"`
	Revision   uint64 `json:"revision"`
	RootHash   string `json:"rootHash"`
	LeasePath  string `json:"path"`
}

func SendNotification(ctx context.Context, repository string, repo *RepositoryConfig, leasePath string, newRootHash string, tag gw.RepositoryTag, revision uint64) {
	var err error

	// connect to message broker
	if kafkaClient == nil {
		opts := []kgo.Opt{
			// longer timeout to maintain connection
			kgo.ConnIdleTimeout(3*time.Minute),
			kgo.SeedBrokers(repo.Events.Broker...),
			kgo.ConsumeTopics(repo.Events.Topic),
			kgo.DefaultProduceTopic(repo.Events.Topic),
			kgo.SASL(scram.Auth{
				User: repo.Events.Username,
				Pass: repo.Events.Password,
			}.AsSha512Mechanism()),
		}
		kafkaClient, err = kgo.NewClient(opts...)
		if err != nil {
			gw.LogC(ctx, "actions", gw.LogError).Err(err).Msg("Unable to connect to message broker")
			return
		}
	}

	// compose and publih message
	msg := kafkaMsg{
		Repository: repository,
		Revision:   revision,
		RootHash:   newRootHash,
		LeasePath:  leasePath,
	}
	msgstr, err := json.Marshal(msg)
	if err != nil {
		gw.LogC(ctx, "actions", gw.LogError).Err(err).Msg("Failed to marshal json message")
		return
	}
	record := &kgo.Record{Topic: repo.Events.Topic, Value: []byte(msgstr)}
	if err := kafkaClient.ProduceSync(ctx, record).FirstErr(); err != nil {
		gw.LogC(ctx, "actions", gw.LogError).Err(err).Msg("Failed to publish to message broker")
		return
	}
	gw.LogC(ctx, "actions", gw.LogInfo).Msg("Successfully published notification to message broker")
}

func SendToTelegraf(ctx context.Context, repo *RepositoryConfig, repository string, revision uint64) {
	line := repo.Telegraf.Line
	if line == "" { // nothing to log
		return
	}
	line = strings.Replace(line, "REPOSITORY", repository, -1)
	line = strings.Replace(line, "REVISION", fmt.Sprintf("%d", revision), -1)
	line = strings.Replace(line, "TIME", fmt.Sprintf("%d", time.Now().UnixNano()), -1)

	con, err := net.Dial("udp", fmt.Sprintf("%s:%d", repo.Telegraf.Host, repo.Telegraf.Port))
	if err != nil {
		gw.LogC(ctx, "actions", gw.LogError).Err(err).Msg("Failed to create UDP socket to telegraf")
		return
	}
	con.Write([]byte(line))
	con.Close()
	gw.LogC(ctx, "actions", gw.LogInfo).Str("measurement", line).Msg("Sent to telegraf")
}
