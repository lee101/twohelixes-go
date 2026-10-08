package twohelixes

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

type batch struct {
	SiteID string  `json:"site_id"`
	Events []Event `json:"events"`
}

func collector(t *testing.T) (*httptest.Server, func() []batch) {
	var mu sync.Mutex
	var got []batch
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/collect" || r.Method != http.MethodPost {
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
		}
		var b batch
		if err := json.NewDecoder(r.Body).Decode(&b); err != nil {
			t.Error(err)
		}
		mu.Lock()
		got = append(got, b)
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)
	return srv, func() []batch {
		mu.Lock()
		defer mu.Unlock()
		return append([]batch(nil), got...)
	}
}

func TestTrackBatchesAndShape(t *testing.T) {
	srv, got := collector(t)
	c, err := New(Config{WriteKey: "thw_test", APIURL: srv.URL, FlushAt: 100, FlushInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	c.Identify("user_1", map[string]any{"plan": "free"}, WithClient("c1"))
	c.Track(PaywallOpen, map[string]any{"plan": "pro"}, WithUser("user_1"), WithClient("c1"), WithSurface("export_button"))
	c.Purchase(19, "usd", "txn_9", map[string]any{"plan": "pro"}, WithUser("user_1"))
	if err := c.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	batches := got()
	if len(batches) != 1 || batches[0].SiteID != "thw_test" || len(batches[0].Events) != 3 {
		t.Fatalf("batches = %+v", batches)
	}
	ev := batches[0].Events
	if ev[0].Type != "identify" || ev[0].UserID != "user_1" || ev[0].Traits["plan"] != "free" {
		t.Fatalf("identify = %+v", ev[0])
	}
	if ev[1].Event != "paywall_open" || ev[1].Props["surface"] != "export_button" || ev[1].ClientID != "c1" || ev[1].TS == 0 {
		t.Fatalf("paywall = %+v", ev[1])
	}
	if ev[2].Event != "purchase" || ev[2].EventID != "txn_9" || ev[2].Props["currency"] != "USD" || ev[2].Props["revenue"] != 19.0 || ev[2].ClientID != "user_1" {
		t.Fatalf("purchase = %+v", ev[2])
	}
}

func TestFlushAtAndChunking(t *testing.T) {
	srv, got := collector(t)
	c, _ := New(Config{WriteKey: "thw_test", APIURL: srv.URL, FlushAt: 5000, FlushInterval: time.Hour, MaxQueue: 10000})
	for i := 0; i < 4500; i++ {
		c.Track("tick", nil)
	}
	if err := c.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	sizes := []int{}
	for _, b := range got() {
		sizes = append(sizes, len(b.Events))
	}
	if len(sizes) != 3 || sizes[0] != 2000 || sizes[1] != 2000 || sizes[2] != 500 {
		t.Fatalf("chunks = %v", sizes)
	}

	c2, _ := New(Config{WriteKey: "thw_test", APIURL: srv.URL, FlushAt: 3, FlushInterval: time.Hour})
	defer c2.Close(context.Background())
	for i := 0; i < 3; i++ {
		c2.Track("auto", nil)
	}
	deadline := time.Now().Add(2 * time.Second)
	for len(got()) < 4 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if len(got()) != 4 {
		t.Fatalf("flushAt did not trigger: %d batches", len(got()))
	}
}

func TestUnreachableNeverPanics(t *testing.T) {
	var errs []error
	var mu sync.Mutex
	c, _ := New(Config{WriteKey: "thw_test", APIURL: "http://127.0.0.1:1", FlushInterval: time.Hour,
		HTTPClient: &http.Client{Timeout: 200 * time.Millisecond},
		OnError:    func(err error) { mu.Lock(); errs = append(errs, err); mu.Unlock() }})
	c.Track("x", nil)
	if err := c.Close(context.Background()); err == nil {
		t.Fatal("expected error from unreachable collector")
	}
}

func TestReports(t *testing.T) {
	var seen *http.Request
	var body map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = r
		if r.Method == http.MethodPost {
			_ = json.NewDecoder(r.Body).Decode(&body)
		}
		if r.URL.Path == "/v1/analytics/revenue" {
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"error":"forbidden","detail":"not your site"}`))
			return
		}
		_, _ = w.Write([]byte(`{"steps":[{"event":"page_view:/signup","sessions":3}]}`))
	}))
	defer srv.Close()
	r := NewReports("th_test")
	r.APIURL = srv.URL
	ctx := context.Background()

	out, err := r.Funnel(ctx, "site_1", 7, "page_view:/signup", "sign_up_completed|sign_in_completed", "purchase")
	if err != nil {
		t.Fatal(err)
	}
	if seen.Header.Get("Authorization") != "Bearer th_test" {
		t.Fatalf("auth = %q", seen.Header.Get("Authorization"))
	}
	q := seen.URL.Query()
	if q.Get("steps") != "page_view:/signup,sign_up_completed|sign_in_completed,purchase" || q.Get("days") != "7" || q.Get("site_id") != "site_1" {
		t.Fatalf("query = %v", q)
	}
	if out["steps"] == nil {
		t.Fatalf("out = %v", out)
	}

	if _, err := r.Ask(ctx, "site_1", "where do sign-ups come from?", 0); err != nil {
		t.Fatal(err)
	}
	if seen.URL.Path != "/v1/query" || body["analytics_site_id"] != "site_1" || body["days"] != 30.0 {
		t.Fatalf("ask = %s %v", seen.URL.Path, body)
	}

	_, err = r.Revenue(ctx, "site_1", 30)
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.Status != 403 || apiErr.Code != "forbidden" || apiErr.Detail != "not your site" {
		t.Fatalf("err = %v", err)
	}
}
