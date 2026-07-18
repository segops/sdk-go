// Package segops is the official Go SDK for SegOps behavioral segmentation.
//
// Usage:
//
//	client := segops.New(segops.Options{
//	    APIURL: "https://api.segops.ai",
//	    APIKey: "sk_...",
//	})
//	defer client.Shutdown(context.Background())
//
//	client.Track(segops.Event{
//	    UserID:    "user-123",
//	    EventType: "page_viewed",
//	    Payload:   map[string]any{"path": "/home"},
//	})
package segops

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	defaultBatchSize    = 20
	defaultFlushEvery   = 5 * time.Second
	defaultMaxRetries   = 3
	defaultHTTPTimeout  = 10 * time.Second
)

// Event is a single user event in the canonical SegOps schema.
type Event struct {
	UserID     string         `json:"user_id"`
	EventType  string         `json:"event_type"`
	OccurredAt string         `json:"occurred_at,omitempty"` // RFC3339; defaults to now
	Payload    map[string]any `json:"payload,omitempty"`
}

// Context represents a user identity / trait update.
// It is translated to a context_identified event.
type Context struct {
	UserID string         `json:"user_id"`
	Traits map[string]any `json:"traits"`
}

// Options configures the SegOps client.
type Options struct {
	// APIURL is the base URL of your SegOps deployment, e.g. "https://api.segops.ai".
	APIURL string
	// APIKey is the API key with ingest permission (sk_…).
	APIKey string
	// BatchSize is the maximum number of events queued before an automatic flush. Default: 20.
	BatchSize int
	// FlushEvery is the periodic flush interval. Default: 5s.
	FlushEvery time.Duration
	// MaxRetries is the number of retry attempts on transient failures. Default: 3.
	MaxRetries int
	// Logger is used for error output. Defaults to the standard logger.
	Logger *log.Logger
}

// Client sends user events to SegOps asynchronously.
// All methods are safe for concurrent use.
type Client struct {
	opts       Options
	httpClient *http.Client

	mu    sync.Mutex
	queue []Event

	flushCh chan struct{}
	stopCh  chan struct{}
	wg      sync.WaitGroup
}

// New creates and starts a SegOps client with the given options.
// Call Shutdown when the process exits to flush remaining events.
func New(opts Options) *Client {
	if opts.BatchSize <= 0 {
		opts.BatchSize = defaultBatchSize
	}
	if opts.FlushEvery <= 0 {
		opts.FlushEvery = defaultFlushEvery
	}
	if opts.MaxRetries <= 0 {
		opts.MaxRetries = defaultMaxRetries
	}
	if opts.Logger == nil {
		opts.Logger = log.Default()
	}
	opts.APIURL = strings.TrimRight(opts.APIURL, "/")

	c := &Client{
		opts:       opts,
		httpClient: &http.Client{Timeout: defaultHTTPTimeout},
		flushCh:    make(chan struct{}, 1),
		stopCh:     make(chan struct{}),
	}

	c.wg.Add(1)
	go c.loop()
	return c
}

// Track enqueues a user event. The event is sent in the background.
func (c *Client) Track(e Event) {
	if e.OccurredAt == "" {
		e.OccurredAt = time.Now().UTC().Format(time.RFC3339)
	}
	if e.Payload == nil {
		e.Payload = map[string]any{}
	}

	c.mu.Lock()
	c.queue = append(c.queue, e)
	full := len(c.queue) >= c.opts.BatchSize
	c.mu.Unlock()

	if full {
		select {
		case c.flushCh <- struct{}{}:
		default:
		}
	}
}

// Identify records user trait updates as a context_identified event.
func (c *Client) Identify(ctx Context) {
	c.Track(Event{
		UserID:    ctx.UserID,
		EventType: "context_identified",
		Payload:   ctx.Traits,
	})
}

// Flush sends all buffered events immediately. Blocks until complete.
func (c *Client) Flush(ctx context.Context) error {
	c.mu.Lock()
	if len(c.queue) == 0 {
		c.mu.Unlock()
		return nil
	}
	batch := c.drain()
	c.mu.Unlock()
	return c.send(ctx, batch)
}

// Shutdown flushes remaining events and stops the background goroutine.
// Always call this before the process exits.
func (c *Client) Shutdown(ctx context.Context) error {
	close(c.stopCh)
	c.wg.Wait()
	return c.Flush(ctx)
}

// ── internal ──────────────────────────────────────────────────────────────────

func (c *Client) loop() {
	defer c.wg.Done()
	ticker := time.NewTicker(c.opts.FlushEvery)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.flushNow()
		case <-c.flushCh:
			c.flushNow()
		case <-c.stopCh:
			return
		}
	}
}

func (c *Client) flushNow() {
	c.mu.Lock()
	if len(c.queue) == 0 {
		c.mu.Unlock()
		return
	}
	batch := c.drain()
	c.mu.Unlock()

	if err := c.send(context.Background(), batch); err != nil {
		c.opts.Logger.Printf("[SegOps] flush error: %v", err)
	}
}

// drain moves queued events out of the queue without holding mu.
func (c *Client) drain() []Event {
	batch := make([]Event, len(c.queue))
	copy(batch, c.queue)
	c.queue = c.queue[:0]
	return batch
}

func (c *Client) send(ctx context.Context, batch []Event) error {
	body, err := json.Marshal(map[string]any{"events": batch})
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	url := c.opts.APIURL + "/api/ingestion/track/batch/"
	var lastErr error
	for attempt := 0; attempt <= c.opts.MaxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(attempt) * 500 * time.Millisecond):
			}
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "ApiKey "+c.opts.APIKey)

		resp, err := c.httpClient.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		resp.Body.Close()

		if resp.StatusCode == http.StatusAccepted || resp.StatusCode == http.StatusOK {
			return nil
		}
		if resp.StatusCode >= 500 {
			lastErr = fmt.Errorf("HTTP %d", resp.StatusCode)
			continue // retry on 5xx
		}
		return fmt.Errorf("SegOps: HTTP %d (non-retryable)", resp.StatusCode)
	}
	return fmt.Errorf("SegOps: after %d retries: %w", c.opts.MaxRetries, lastErr)
}
