// Package twohelixes is a dependency-free client for twoHelixes product
// analytics: batched event ingest and the reporting API.
package twohelixes

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	DefaultAPIURL = "https://twohelixes.com"
	MaxBatch      = 2000
)

const (
	PageView             = "page_view"
	SignupView           = "signup_view"
	SignUpStarted        = "sign_up_started"
	SignUpCompleted      = "sign_up_completed"
	SignInStarted        = "sign_in_started"
	SignInCompleted      = "sign_in_completed"
	PaywallOpen          = "paywall_open"
	PaywallSignupClicked = "paywall_signup_clicked"
	BeginCheckout        = "begin_checkout"
	Purchase             = "purchase"
	Refund               = "refund"
)

type Event struct {
	Type         string         `json:"type,omitempty"`
	Event        string         `json:"event"`
	ClientID     string         `json:"client_id"`
	UserID       string         `json:"user_id,omitempty"`
	SessionID    string         `json:"session_id,omitempty"`
	EventID      string         `json:"event_id,omitempty"`
	TS           int64          `json:"ts"`
	PageLocation string         `json:"page_location,omitempty"`
	Referrer     string         `json:"referrer,omitempty"`
	Props        map[string]any `json:"props,omitempty"`
	Traits       map[string]any `json:"traits,omitempty"`
}

type Config struct {
	WriteKey      string
	APIURL        string
	FlushAt       int
	FlushInterval time.Duration
	MaxQueue      int
	HTTPClient    *http.Client
	OnError       func(error)
}

type Client struct {
	cfg      Config
	endpoint string
	clientID string

	mu    sync.Mutex
	queue []Event

	sendMu  sync.Mutex
	kick    chan struct{}
	stop    chan struct{}
	done    chan struct{}
	stopped sync.Once
}

// New starts a background flusher. Call Close before exit.
func New(cfg Config) (*Client, error) {
	if cfg.WriteKey == "" {
		cfg.WriteKey = os.Getenv("TWOHELIXES_WRITE_KEY")
	}
	if cfg.WriteKey == "" {
		return nil, errors.New("twohelixes: write key required")
	}
	if cfg.APIURL == "" {
		cfg.APIURL = envOr("TWOHELIXES_API_URL", DefaultAPIURL)
	}
	if cfg.FlushAt <= 0 {
		cfg.FlushAt = 20
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 5 * time.Second
	}
	if cfg.MaxQueue <= 0 {
		cfg.MaxQueue = 10000
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{Timeout: 10 * time.Second}
	}
	if cfg.OnError == nil {
		cfg.OnError = func(err error) { log.Printf("twohelixes: %v", err) }
	}
	c := &Client{
		cfg:      cfg,
		endpoint: strings.TrimRight(cfg.APIURL, "/") + "/v1/collect",
		clientID: randomID(),
		kick:     make(chan struct{}, 1),
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	go c.loop()
	return c, nil
}

type Option func(*Event)

func WithUser(id string) Option       { return func(e *Event) { e.UserID = id } }
func WithClient(id string) Option     { return func(e *Event) { e.ClientID = id } }
func WithSession(id string) Option    { return func(e *Event) { e.SessionID = id } }
func WithEventID(id string) Option    { return func(e *Event) { e.EventID = id } }
func WithURL(url string) Option       { return func(e *Event) { e.PageLocation = url } }
func WithReferrer(url string) Option  { return func(e *Event) { e.Referrer = url } }
func WithTime(t time.Time) Option     { return func(e *Event) { e.TS = t.UnixMilli() } }
func WithSurface(name string) Option  { return WithProp("surface", name) }
func WithProp(k string, v any) Option { return func(e *Event) { setProp(e, k, v) } }

func setProp(e *Event, k string, v any) {
	if e.Props == nil {
		e.Props = map[string]any{}
	}
	e.Props[k] = v
}

// Track enqueues an event. It never blocks on the network.
func (c *Client) Track(name string, props map[string]any, opts ...Option) {
	e := Event{Event: name, TS: time.Now().UnixMilli()}
	if len(props) > 0 {
		e.Props = make(map[string]any, len(props))
		for k, v := range props {
			e.Props[k] = v
		}
	}
	for _, opt := range opts {
		opt(&e)
	}
	c.Enqueue(e)
}

func (c *Client) Identify(userID string, traits map[string]any, opts ...Option) {
	e := Event{Type: "identify", Event: "identify", UserID: userID, Traits: traits, TS: time.Now().UnixMilli()}
	for _, opt := range opts {
		opt(&e)
	}
	c.Enqueue(e)
}

// Purchase records revenue in major units; transactionID dedupes retries.
func (c *Client) Purchase(revenue float64, currency, transactionID string, props map[string]any, opts ...Option) {
	merged := map[string]any{"revenue": revenue, "currency": strings.ToUpper(currency)}
	if transactionID != "" {
		merged["transaction_id"] = transactionID
		opts = append([]Option{WithEventID(transactionID)}, opts...)
	}
	for k, v := range props {
		merged[k] = v
	}
	c.Track(Purchase, merged, opts...)
}

func (c *Client) Enqueue(e Event) {
	if e.ClientID == "" {
		e.ClientID = e.UserID
	}
	if e.ClientID == "" {
		e.ClientID = c.clientID
	}
	if e.TS == 0 {
		e.TS = time.Now().UnixMilli()
	}
	c.mu.Lock()
	if len(c.queue) >= c.cfg.MaxQueue {
		c.mu.Unlock()
		c.cfg.OnError(errors.New("queue full, event dropped"))
		return
	}
	c.queue = append(c.queue, e)
	full := len(c.queue) >= c.cfg.FlushAt
	c.mu.Unlock()
	if full {
		select {
		case c.kick <- struct{}{}:
		default:
		}
	}
}

func (c *Client) loop() {
	defer close(c.done)
	ticker := time.NewTicker(c.cfg.FlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.stop:
			return
		case <-ticker.C:
		case <-c.kick:
		}
		if err := c.Flush(context.Background()); err != nil {
			c.cfg.OnError(err)
		}
	}
}

// Flush sends everything queued, in chunks of at most MaxBatch.
func (c *Client) Flush(ctx context.Context) error {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	c.mu.Lock()
	pending := c.queue
	c.queue = nil
	c.mu.Unlock()
	var errs []error
	for len(pending) > 0 {
		n := min(len(pending), MaxBatch)
		if err := c.send(ctx, pending[:n]); err != nil {
			errs = append(errs, err)
		}
		pending = pending[n:]
	}
	return errors.Join(errs...)
}

func (c *Client) send(ctx context.Context, events []Event) error {
	body, err := json.Marshal(map[string]any{"site_id": c.cfg.WriteKey, "events": events})
	if err != nil {
		return err
	}
	var last error
	for attempt := 0; attempt < 2; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(500 * time.Millisecond):
			}
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := c.cfg.HTTPClient.Do(req)
		if err != nil {
			last = err
			continue
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode < 300 {
			return nil
		}
		last = fmt.Errorf("collect: HTTP %d", resp.StatusCode)
		if resp.StatusCode < 500 {
			return last
		}
	}
	return last
}

// Close flushes and stops the background goroutine.
func (c *Client) Close(ctx context.Context) error {
	c.stopped.Do(func() { close(c.stop) })
	select {
	case <-c.done:
	case <-ctx.Done():
		return ctx.Err()
	}
	return c.Flush(ctx)
}

func randomID() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func envOr(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}
