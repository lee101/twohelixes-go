# twohelixes-go

Tiny, standard-library-only Go client for [twoHelixes](https://twohelixes.com)
product analytics: batched, non-blocking event ingest plus the reporting API
(summary, funnels, paths, revenue, natural-language questions).

```sh
go get github.com/lee101/twohelixes-go
```

## Track

```go
th, err := twohelixes.New(twohelixes.Config{WriteKey: "thw_xxx"}) // or TWOHELIXES_WRITE_KEY
if err != nil { log.Fatal(err) }
defer th.Close(context.Background())

th.Track(twohelixes.SignUpStarted, nil, twohelixes.WithClient(anonID), twohelixes.WithSurface("hero_cta"))
th.Identify(user.ID, map[string]any{"plan": "free"}, twohelixes.WithClient(anonID))
th.Track(twohelixes.SignUpCompleted, map[string]any{"method": "google"}, twohelixes.WithUser(user.ID))
th.Track(twohelixes.PaywallOpen, nil, twohelixes.WithUser(user.ID), twohelixes.WithSurface("export_button"))
th.Track(twohelixes.BeginCheckout, map[string]any{"plan": "pro", "value": 19, "currency": "USD"},
	twohelixes.WithUser(user.ID), twohelixes.WithSurface("export_button"))

// Stripe webhook: transaction id doubles as the idempotency key.
th.Purchase(19, "USD", invoice.ID, map[string]any{"plan": "pro", "surface": "export_button"},
	twohelixes.WithUser(user.ID))
```

`Track` never blocks on the network. Events queue in memory and flush every
`FlushInterval` (5s) or at `FlushAt` (20) events, in chunks of at most 2,000,
with one retry on 5xx/network errors. `Flush(ctx)` forces a send; `Close(ctx)`
flushes and stops the goroutine. Errors go to `Config.OnError` (default: `log`).

`client_id` falls back to the user id, then to a random per-process id.
Options: `WithUser`, `WithClient`, `WithSession`, `WithEventID`, `WithURL`,
`WithReferrer`, `WithTime`, `WithSurface`, `WithProp`.

Canonical SaaS events are exported as constants: `PageView`, `SignupView`,
`SignUpStarted`, `SignUpCompleted`, `SignInStarted`, `SignInCompleted`,
`PaywallOpen`, `PaywallSignupClicked`, `BeginCheckout`, `Purchase`, `Refund`.
See the [event taxonomy](https://github.com/lee101/twohelixes-examples#event-taxonomy).

## Report

```go
r := twohelixes.NewReports("") // TWOHELIXES_API_KEY (th_...), TWOHELIXES_API_URL
ctx := context.Background()
sites, _ := r.Sites(ctx)
funnel, _ := r.Funnel(ctx, "site_id", 30,
	"page_view", "sign_up_started", "sign_up_completed", "paywall_open", "begin_checkout", "purchase")
revenue, _ := r.Revenue(ctx, "site_id", 30)
answer, _ := r.Ask(ctx, "site_id", "which paywall surface converts best?", 30)
```

Funnel steps are sent verbatim, so server-side qualifiers such as
`page_view:/signup` and `sign_up_completed|sign_in_completed` work unchanged.
Non-2xx responses return `*twohelixes.APIError{Status, Code, Detail}`.

## Agents

The [twohelixes-cli](https://github.com/lee101/twohelixes-cli) binary includes
an MCP server exposing the same reports to Claude Code and other agents:

```sh
claude mcp add twohelixes -e TWOHELIXES_API_KEY=th_xxx -- twohelixes-cli mcp
```

## License

MIT
