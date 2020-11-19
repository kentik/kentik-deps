package client

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
	"github.com/kentik/tagstreaming/logging"
	"github.com/kentik/tagstreaming/models/lookups"
	lookups_queue "github.com/kentik/tagstreaming/queue/ingest/lookups"
)

// TagLookupsClient is the remote interface into the tag-api service's tag lookups endpoints.
// This caches previous responses.
// - threadsafe
type TagLookupsClient struct {
	log              logging.Logger
	httpClient       *http.Client
	logPrefix        string
	lookupURLFormat  string
	legacyLookupURL  string
	enabledAfterTime int64 // when we receive an http error, we atomically set a unix timestamp of when the client is allowed: +10 seconds
	cache            *lookups.Cache
	kafkaBrokerList  []string
	quitChan         chan interface{}

	options Options
}

// Options provides all options for TagLookupsClient
type Options struct {
	Disabled             bool          // whether this is disabled - if true, return empty lookups
	HTTPTimeoutDuration  time.Duration // how many seconds for http timeouts
	ErrorLockoutDuration time.Duration // how many seconds to disable client after http errors
}

// NewTagLookupsClient returns a new TagLookupsClient
func NewTagLookupsClient(log logging.Logger, logPrefix string, serverHostPort string, options Options, kafkaBrokerList []string) (*TagLookupsClient, error) {
	var httpClient *http.Client
	if !options.Disabled {
		httpClient = &http.Client{
			Timeout: options.HTTPTimeoutDuration,
		}
	}

	return &TagLookupsClient{
		log:             log,
		logPrefix:       logPrefix,
		lookupURLFormat: fmt.Sprintf("http://%s/company/%%d/tag_lookups", serverHostPort),
		legacyLookupURL: fmt.Sprintf("http://%s/legacy_tag_lookups", serverHostPort),
		cache:           lookups.NewCache(),
		kafkaBrokerList: kafkaBrokerList,
		httpClient:      httpClient,
		quitChan:        make(chan interface{}, 0),
		options:         options,
	}, nil
}

// Start the background processing loop
// - returns immediately, processing in the background
func (c *TagLookupsClient) Start() error {
	// fetch the legacy lookups
	legacyLookups, err := c.fetchLegacyTagLookups()
	if err != nil {
		c.log.Errorf(c.logPrefix, "Failed to fetch legacy tag lookups. Starting 10 second retry loop to fetch legacy lookups: %s", err)

		// start retry loop
		go func() {
			for {
				time.Sleep(10 * time.Second)
				legacyLookups, err := c.fetchLegacyTagLookups()
				if err != nil {
					c.log.Errorf(c.logPrefix, "Failed retry fetch of legacy tag lookups - trying again in 10 seconds: %s", err)
					continue
				}

				c.cache.UpdateWithLegacyTagLookups(legacyLookups)
				return
			}
		}()
	} else {
		c.cache.UpdateWithLegacyTagLookups(legacyLookups)
	}

	lookupsConsumer, err := lookups_queue.NewConsumer(c.log, c.logPrefix, c.kafkaBrokerList)
	if err != nil {
		return fmt.Errorf("Error creating LookupsConsumer: %s", err)
	}

	go lookupsConsumer.Consume(sarama.OffsetNewest)

	lookupChangesChan, err := lookupsConsumer.Messages()
	if err != nil {
		return fmt.Errorf("Error getting Messages channel from lookups consumer: %s", err)
	}

	// background run loop:
	// - evicts from cache any tag-lookups that weren't found, so they'll be attempted again later
	// - handle tag lookup change messages
	maintenanceTimer := time.After(randDurationMinutes(4, 6))
	lastMaintRunTime := time.Now()
	go func() {
		for {
			select {
			case <-maintenanceTimer:
				// find lookups and reverse lookups that have default values, in case we got a bad/incomplete response from tag-api
				if evictedCount := c.cache.Clean(); evictedCount > 0 {
					c.log.Infof(c.logPrefix, "clean: evicted %d empty items from cache", evictedCount)
				}

				// replay the queue with overlap
				replayTimeAgo := time.Now().Sub(lastMaintRunTime) + 5*time.Minute
				c.log.Debugf(c.logPrefix, "Ensuring cache is proper by replaying the last %s", replayTimeAgo)
				if err := c.replayLookupsChangedQueue(replayTimeAgo); err != nil {
					retryTime := randDurationMinutes(1, 2)
					maintenanceTimer = time.After(retryTime)
					c.log.Errorf(c.logPrefix, "Error replaying queue from last %s - trying again in %s: %s", replayTimeAgo, retryTime, err)
					continue
				}

				// pick new random maintenance time
				maintenanceTimer = time.After(randDurationMinutes(4, 6))
				lastMaintRunTime = time.Now()

			case msg := <-lookupChangesChan:
				c.cache.HandleLookupCaseChanges(msg.TagLookups)

			case <-c.quitChan:
				c.log.Infof(c.logPrefix, "TagLookupsClient run loop stopping")
				lookupsConsumer.Close()
				return
			}
		}
	}()

	return nil
}

// Stop stops any background threads
func (c *TagLookupsClient) Stop() {
	select {
	case <-c.quitChan:
		// already closed
		break

	default:
		close(c.quitChan)
	}
}

// IsLookupID returns whether the input id is a tag lookup
// now that we manage legacy tag lookups, everything is a tag lookup
func IsLookupID(id uint64) bool {
	// - tag lookups are > 1B
	// - legacy tagIDs are < 1B
	return true
}

// NewRequest creates a new tag lookups request for the tag-api service
func (c *TagLookupsClient) NewRequest() *lookups.TagLookupsRequest {
	return lookups.NewTagLookupsRequest()
}

// fetch the legacy tag lookups
func (c *TagLookupsClient) fetchLegacyTagLookups() (map[uint32]map[uint64]uint64, error) {
	start := time.Now()
	resp, err := c.httpClient.Get(c.legacyLookupURL)
	if err != nil {
		return nil, fmt.Errorf("Error fetching legacy tag lookups from remote tag-api service: %s", err)
	}

	defer resp.Body.Close()
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("Error reading response body during legacy tag lookup request from tag-api: %s", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Error response from remote tag-api service - status-code: %d", resp.StatusCode)
	}

	serverResponse := lookups.LegacyAllTagLookupsResponse{}
	if err := serverResponse.Unmarshal(respBody); err != nil {
		return nil, fmt.Errorf("Error unmarshalling LegacyAllTagLookupsResponse: %s", err)
	}

	ret := make(map[uint32]map[uint64]uint64)
	lookupsCount := 0
	for companyID := range serverResponse.LegacyCompanyTagLookups {
		companyLookups := make(map[uint64]uint64)
		for legacyTagID, lookupID := range serverResponse.LegacyCompanyTagLookups[companyID].LegacyTagLookups {
			companyLookups[legacyTagID] = lookupID
		}
		ret[companyID] = companyLookups
		lookupsCount += len(companyLookups)
	}
	c.log.Infof(c.logPrefix, "Successfully loaded %d legacy tag lookups for %d companies in %s", lookupsCount, len(serverResponse.LegacyCompanyTagLookups), time.Now().Sub(start))
	return ret, nil
}

// FetchLookups attempts to fetch the input tag lookups from the remote tag-api service.
func (c *TagLookupsClient) FetchLookups(companyID uint32, request *lookups.TagLookupsRequest) (*lookups.TagLookupsResponse, error) {
	if c.options.Disabled {
		c.log.Debugf(c.logPrefix, "Remote tag lookups are disabled (by configuration) - returning empty lookups")
		return lookups.NewTagLookupsResponse(), nil
	}
	if !c.isEnabled() {
		c.log.Debugf(c.logPrefix, "Remote tag lookups are disabled (briefly, from tag-api server err0r) - returning empty lookups")
		return lookups.NewTagLookupsResponse(), nil
	}

	// filter the lookups based on cache
	// - response keys will match casing of request's
	cachedResponse, legacyTagLookups := c.cache.HandleRequest(companyID, request)

	// see if we even need to go to the server
	if request.IsEmpty() {
		// can service this locally
		c.log.Debugf(c.logPrefix, "Serviced tag lookups for company %d entirely from cache", companyID)
		return cachedResponse, nil
	}

	// cache couldn't handle the full lookup - need server
	// - response keys casing will match request key's
	start := time.Now()
	url := fmt.Sprintf(c.lookupURLFormat, companyID)
	c.log.Debugf(c.logPrefix, "Fetching tag lookups from tag-api at %s", url)
	requestBytes, err := request.Marshal()
	if err != nil {
		c.disable()
		return nil, fmt.Errorf("Error marshalling TagLookupsRequest: %s", err)
	}
	resp, err := c.httpClient.Post(url, "application/protobuf; proto=com.kentik.tagstreaming.models.lookups", bytes.NewBuffer(requestBytes))
	if err != nil {
		c.disable()
		return nil, fmt.Errorf("Error fetching tag lookups from remote tag-api service: %s", err)
	}

	defer resp.Body.Close()
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		c.disable()
		return nil, fmt.Errorf("Error reading response body during tag lookup request from tag-api: %s", err)
	}

	if resp.StatusCode != http.StatusOK {
		c.disable()
		return nil, fmt.Errorf("Error response from remote tag-api service - status-code: %d", resp.StatusCode)
	}

	c.log.Debugf(c.logPrefix, "Received %d bytes from tag-api", len(respBody))
	serverResponse := lookups.NewTagLookupsResponse()
	if err := serverResponse.Unmarshal(respBody); err != nil {
		c.disable()
		return nil, fmt.Errorf("Error unmarshalling TagLookupsResponse: %s", err)
	}

	// populate our cache from the server response
	c.cache.UpdateWithResponse(companyID, serverResponse)

	// map any legacy IDs back into the response
	for legacyLookupID, lookupID := range legacyTagLookups {
		serverResponse.Lookups[legacyLookupID] = serverResponse.Lookups[lookupID]
	}

	// populate response with our cache results from earlier
	serverResponse.Merge(cachedResponse)

	c.log.Debugf(c.logPrefix, "Fetched tag lookups cache from server for company id %d in %s", companyID, time.Now().Sub(start))
	return serverResponse, nil
}

// replay the tag lookups update queue over the the past <input> interval, to clean up any
// cache races that left us with old data. As long as tag-api keeps its cache current, and we overlap
// replay window over polling window, we'll bring the cache to current.
func (c *TagLookupsClient) replayLookupsChangedQueue(durationAgo time.Duration) error {
	lookupsConsumer, err := lookups_queue.NewConsumer(c.log, fmt.Sprintf("%s(queue-replay) ", c.logPrefix), c.kafkaBrokerList)
	if err != nil {
		return fmt.Errorf("Error creating LookupsConsumer in replay/update poll: %s", err)
	}
	defer lookupsConsumer.Close()

	offset, err := lookupsConsumer.OffsetNearTime(time.Now().Add(-1 * durationAgo))
	if err != nil {
		return fmt.Errorf("Error fetching offset near time: %s", err)
	}
	if offset == -1 {
		// nothing in the queue
		return nil
	}

	go lookupsConsumer.Consume(offset)

	lookupChangesChan, err := lookupsConsumer.PastMessages()
	if err != nil {
		return fmt.Errorf("Error getting Messages channel from lookups consumer: %s", err)
	}

	msgCount := 0
	for msg := range lookupChangesChan {
		msgCount++
		c.cache.HandleLookupCaseChanges(msg.TagLookups)
	}
	c.log.Debugf(c.logPrefix, "Replayed %d messages from %s ago", msgCount, durationAgo)
	return nil
}

func (c *TagLookupsClient) isEnabled() bool {
	return time.Now().Unix() >= atomic.LoadInt64(&c.enabledAfterTime)
}

func (c *TagLookupsClient) disable() {
	if c.options.ErrorLockoutDuration > 0 {
		c.log.Warnf(c.logPrefix, "Disabling tag lookups for %s", c.options.ErrorLockoutDuration)
		atomic.StoreInt64(&c.enabledAfterTime, time.Now().Add(c.options.ErrorLockoutDuration).Unix())
	}
}

// return a time Duration between the input seconds ranges
func randDurationSeconds(from int, to int) time.Duration {
	if to < from {
		to = from
	}
	milliseconds := from*1000 + int(rand.Float32()*float32((to-from)*1000))
	return time.Duration(time.Duration(milliseconds) * time.Millisecond)
}

// return a time Duration between the input minute ranges
func randDurationMinutes(from int, to int) time.Duration {
	return randDurationSeconds(from*60, to*60)
}
