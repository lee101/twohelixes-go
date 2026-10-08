package twohelixes

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type APIError struct {
	Status int
	Code   string
	Detail string
}

func (e *APIError) Error() string {
	if e.Detail != "" {
		return fmt.Sprintf("twohelixes: %s (%d): %s", e.Code, e.Status, e.Detail)
	}
	return fmt.Sprintf("twohelixes: %s (%d)", e.Code, e.Status)
}

// Reports reads the reporting API with an account API key (th_...).
type Reports struct {
	APIKey string
	APIURL string
	HTTP   *http.Client
}

func NewReports(apiKey string) *Reports {
	if apiKey == "" {
		apiKey = os.Getenv("TWOHELIXES_API_KEY")
	}
	return &Reports{
		APIKey: apiKey,
		APIURL: envOr("TWOHELIXES_API_URL", DefaultAPIURL),
		HTTP:   &http.Client{Timeout: 90 * time.Second},
	}
}

type Params map[string]string

func (r *Reports) Sites(ctx context.Context) (map[string]any, error) {
	return r.Get(ctx, "sites", "", 0, nil)
}

func (r *Reports) Summary(ctx context.Context, site string, days int) (map[string]any, error) {
	return r.Get(ctx, "summary", site, days, nil)
}

func (r *Reports) Timeseries(ctx context.Context, site string, days int) (map[string]any, error) {
	return r.Get(ctx, "timeseries", site, days, nil)
}

func (r *Reports) Events(ctx context.Context, site string, days, limit int) (map[string]any, error) {
	return r.Get(ctx, "events", site, days, Params{"limit": strconv.Itoa(limit)})
}

func (r *Reports) Schema(ctx context.Context, site string, days int, event string) (map[string]any, error) {
	return r.Get(ctx, "schema", site, days, Params{"event": event})
}

func (r *Reports) Paths(ctx context.Context, site string, days int, p Params) (map[string]any, error) {
	return r.Get(ctx, "paths", site, days, p)
}

func (r *Reports) Revenue(ctx context.Context, site string, days int) (map[string]any, error) {
	return r.Get(ctx, "revenue", site, days, nil)
}

// Funnel passes steps through verbatim, so server-side qualifiers such as
// "page_view:/signup" or "sign_up_completed|sign_in_completed" work as-is.
func (r *Reports) Funnel(ctx context.Context, site string, days int, steps ...string) (map[string]any, error) {
	return r.Get(ctx, "funnel", site, days, Params{"steps": strings.Join(steps, ",")})
}

func (r *Reports) Export(ctx context.Context, site string, days int) (map[string]any, error) {
	return r.Get(ctx, "export", site, days, nil)
}

// Ask runs the natural-language chart agent over the raw event stream.
func (r *Reports) Ask(ctx context.Context, site, question string, days int) (map[string]any, error) {
	body := map[string]any{"q": question, "analytics_site_id": site, "days": orDays(days)}
	return r.do(ctx, http.MethodPost, "/v1/query", body)
}

func (r *Reports) Get(ctx context.Context, endpoint, site string, days int, p Params) (map[string]any, error) {
	q := url.Values{}
	if site != "" {
		q.Set("site_id", site)
		q.Set("days", strconv.Itoa(orDays(days)))
	}
	for k, v := range p {
		if v != "" && v != "0" {
			q.Set(k, v)
		}
	}
	path := "/v1/analytics/" + endpoint
	if len(q) > 0 {
		path += "?" + q.Encode()
	}
	return r.do(ctx, http.MethodGet, path, nil)
}

func (r *Reports) do(ctx context.Context, method, path string, body any) (map[string]any, error) {
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(r.APIURL, "/")+path, reader)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Authorization", "Bearer "+r.APIKey)
	resp, err := r.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 300 {
		apiErr := &APIError{Status: resp.StatusCode, Code: http.StatusText(resp.StatusCode)}
		var parsed map[string]any
		if json.Unmarshal(payload, &parsed) == nil {
			if v, ok := parsed["error"].(string); ok {
				apiErr.Code = v
			}
			if v, ok := parsed["detail"].(string); ok {
				apiErr.Detail = v
			} else if v, ok := parsed["message"].(string); ok {
				apiErr.Detail = v
			}
		}
		return nil, apiErr
	}
	out := map[string]any{}
	if len(payload) > 0 {
		if err := json.Unmarshal(payload, &out); err != nil {
			return nil, fmt.Errorf("twohelixes: decode: %w", err)
		}
	}
	return out, nil
}

func orDays(days int) int {
	if days <= 0 {
		return 30
	}
	return days
}
