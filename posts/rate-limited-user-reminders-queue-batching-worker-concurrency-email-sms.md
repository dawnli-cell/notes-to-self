# Rate-limited user reminders — queue batching, worker concurrency, email/SMS limits

## TL;DR

Put every due reminder on a durable queue, then govern the drain with two separate knobs: worker concurrency, which bounds how many sends are in flight, and a shared token bucket per channel, which bounds how many sends per second the email or SMS provider will actually accept. Batch only where the provider publishes a real batch endpoint, and give every send a deterministic idempotency key so a retry after a timeout can't notify the same user twice. Cron fires the fan-out; it doesn't govern the rate.

## Why reminder fan-out is a rate-limit problem, not a cron problem

I build payment and ledger backends, so I treat an outbound reminder the way I treat a journal entry: it happens once, it reconciles months later, and if a customer asks whether we warned them about a direct debit on the 3rd, I want a row that proves we did and a provider message ID that corroborates it. That bias drives every decision below. If you're sending marketing blasts where a duplicate is merely annoying, you can relax about half of what follows — a missed or doubled payment reminder, on the other hand, generates a support ticket and sometimes a chargeback, and the audit trail is what settles the argument.

The naive design is a single cron entry.

At 09:00 you run a SELECT over the due reminders, loop the rows, and call the email or SMS API for each one. Cron is fine at what it does — it's been running this pattern since Version 7 Unix in 1979 — but it only decides when the loop starts. It has no opinion about how fast the loop runs, no memory of what already went out, and no way to hand a half-finished batch to a second machine.

Then the SELECT returns 41,000 rows.

Now you're against limits that have nothing to do with your CPU. Twilio's US and Canadian long codes send at one message per second by default, and buying throughput means moving to a toll-free number, a short code, or a messaging service that fans across a pool of numbers. SendGrid answers over-quota requests with HTTP 429 and rate-limit headers describing when the window resets. Amazon SES enforces a per-second sending rate on your account in each Region, and going over it doesn't queue politely — it throws. None of those ceilings move because you added workers. Adding workers converts a slow success into a fast pile of 429s, and each 429 you retry without care is a duplicate-delivery risk downstream.

Concurrency is a resource bound. Rate is a contract bound. Treating them as one number is the bug I see most often in reminder pipelines.

So the queue isn't there to make anything faster. It's there to give me a commit point: I write the reminder row and enqueue the job inside the same database transaction (the outbox pattern, if you want the name for it), which means a crash between "we decided to remind this user" and "we told the queue" can't silently drop the obligation. Everything after that commit is a retry problem, and retry problems are tractable.

## How should a Node.js worker split queue concurrency across email and SMS provider limits?

Use one queue per channel, and never one shared queue with a `type` field. Email and SMS have different ceilings, different latencies, different failure semantics, and different business urgency — a backed-up marketing digest must not starve a payment-due text.

Two knobs, two meanings.

In BullMQ the per-worker `concurrency` option controls how many jobs one Node.js process pulls at once, while the queue-level `limiter` (`{ max, duration }`) is enforced in Redis and therefore applies across every worker attached to that queue. Four processes at concurrency 25 gives you 100 jobs in flight, but the limiter still caps how many actually reach the provider per window. pg-boss takes the other route: jobs live in PostgreSQL alongside your data, throughput is shaped by poll interval and batch size, and throttling comes from singleton keys rather than a token bucket. Celery and Sidekiq offer the same split in Python and Ruby, so the shape of the answer travels between ecosystems.

Pick concurrency from Little's law rather than from vibes. In-flight work equals throughput times latency, so if the provider grants you 30 requests per second and the call has a p95 of 400 ms, you need roughly 12 sends in flight to saturate the budget. Setting concurrency to 200 buys nothing except 188 promises parked on the limiter, a fatter memory profile, and a much worse failure mode when the provider slows to 3 seconds and your visibility timeout expires mid-flight. I usually size concurrency at about twice the Little's-law number to absorb latency spikes, then leave it alone.

The limiter has to be shared state or it's a lie. An in-process token bucket is exact on one machine and wrong the moment you scale to three pods, because each pod thinks it owns the whole budget. Either centralise it (Redis, or a `SELECT ... FOR UPDATE` on a counter row if you're already Postgres-only) or divide the provider's quota by the pod count and accept the waste.

Retries need the same discipline. Exponential backoff with jitter is the standard answer, and the reason jitter matters is that a synchronised retry storm reproduces the original overload exactly one backoff window later. Honour `Retry-After` when the provider sends it — that header is the provider telling you the real answer instead of making you guess.

## The sender loop I actually ship

Here's the war story, because it's the part that changed my code rather than my opinions. Last year I merged a legacy reminders table into the new schema and assumed every row carried `contact.phone_e164`. Roughly 6% of the migrated rows still kept the number in a top-level `phone` column, so the marshaller cheerfully produced an empty `To`, and the provider replied with error 21211, "Invalid 'To' Phone Number", on 2,417 messages. That tells you a field was wrong. It does not tell you which upstream write produced it, which template, or which migration batch — and the job dutifully went back on the queue and retried the identical broken payload until it burned through max attempts. I'm not sure why I trusted the shape without a test; I now validate the destination before the job ever consumes a token, and I log the reminder ID alongside the provider's error code so reconciliation has something to join on.

The Go below is the skeleton I keep re-implementing. It separates the rate budget from the worker pool and makes the idempotency key derivable rather than stored, which matters because a derivable key survives a database restore that a random UUID doesn't.

```go
// Package reminders drains due user reminders to an email or SMS provider under
// a per-channel rate budget. Concurrency and the rate limit are separate knobs
// on purpose: one bounds local resources, the other honours the provider contract.
package reminders

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"time"

	"golang.org/x/time/rate"
)

type Reminder struct {
	ID         string
	UserID     string
	TemplateID string
	DueAt      time.Time // truncated to the minute by the scheduler
	To         string    // E.164 for sms, an RFC 5322 address for email
}

type Provider interface {
	// Send must treat idemKey as the provider-side deduplication token.
	Send(ctx context.Context, r Reminder, idemKey string) (providerID string, err error)
}

// Audit is the ledger side: Claim wins a race exactly once, Settle records proof.
type Audit interface {
	Claim(ctx context.Context, idemKey string) (fresh bool, err error)
	Settle(ctx context.Context, idemKey, providerID string) error
}

type Worker struct {
	limiter  *rate.Limiter
	provider Provider
	audit    Audit
}

// perSecond is the provider's contract limit; burst is how much of that budget
// one tick may spend at once. For a US long code that's NewWorker(p, a, 1, 1).
func NewWorker(p Provider, a Audit, perSecond float64, burst int) *Worker {
	return &Worker{rate.NewLimiter(rate.Limit(perSecond), burst), p, a}
}

// idemKey is deterministic: the same logical reminder always hashes to the same
// token, so a retry after a network timeout is a no-op at the provider.
func idemKey(r Reminder) string {
	sum := sha256.Sum256([]byte(strings.Join([]string{
		r.UserID, r.TemplateID, r.DueAt.UTC().Format(time.RFC3339),
	}, "|")))
	return hex.EncodeToString(sum[:])
}

var ErrNoDestination = errors.New("reminder has no destination address")

func (w *Worker) Handle(ctx context.Context, r Reminder) error {
	if r.To == "" {
		return ErrNoDestination // the check I didn't have, and paid for
	}
	// Wait blocks until a token frees up or ctx expires. On expiry the job stays
	// unacknowledged and the queue redelivers it to whichever worker is free.
	if err := w.limiter.Wait(ctx); err != nil {
		return err
	}
	key := idemKey(r)
	fresh, err := w.audit.Claim(ctx, key)
	if err != nil || !fresh {
		return err // already claimed by an earlier attempt: nothing to do
	}
	providerID, err := w.provider.Send(ctx, r, key)
	if err != nil {
		return err
	}
	return w.audit.Settle(ctx, key, providerID)
}
```

Batching is worth a paragraph of its own, because people batch the wrong thing. A batch should mirror what the provider's API actually accepts, never "however many rows were due". SendGrid's v3 mail send takes up to 1,000 personalizations in one request, and SES exposes a bulk send call, so the email publisher chunks against that documented ceiling and treats each chunk as one rate-limited unit. SMS has no equivalent — one message, one request, one token.

```go
// maxPersonalizations mirrors the provider's documented per-request ceiling.
// Chunk against the API contract, not against how many rows the query returned.
const maxPersonalizations = 1000

func chunk(rs []Reminder) [][]Reminder {
	var out [][]Reminder
	for len(rs) > maxPersonalizations {
		out = append(out, rs[:maxPersonalizations])
		rs = rs[maxPersonalizations:]
	}
	if len(rs) > 0 {
		out = append(out, rs)
	}
	return out
}
```

Test the budget, not the happy path. A fake provider that records send timestamps lets you assert the limiter actually holds the line under `-race`, which is the only way I've found to catch a limiter that someone accidentally constructed per-request:

```bash
go test ./reminders -run TestRateBudget -race -count=1
```

## What this design costs, and when to reach for something else

The catch is operational surface. You've now got a queue, a shared limiter, an audit table, and a dead-letter path, and every one of them needs a dashboard. Instrument three things or you're flying blind: queue depth per channel, the ratio of 429s to successful sends, and the age of the oldest unsettled claim in the audit table. That last one is the reconciliation metric — a claim that never settled means a send whose outcome nobody knows, which is precisely the state a ledger person loses sleep over.

| Tool | Backing store | Native rate control | Where I'd reach for it |
| --- | --- | --- | --- |
| BullMQ (Node.js) | Redis | Queue-wide limiter plus per-worker concurrency | Node services already operating Redis |
| pg-boss (Node.js) | PostgreSQL | Singleton keys and poll batch size; no token bucket | Teams who want the job enqueued in the same transaction as the data |
| Inngest | Hosted | Declarative concurrency and throttle per function | Small teams who'd rather not run queue infrastructure |
| Temporal | Its own cluster | Retry policies and timeouts, not a rate budget | Reminders that are one step of a longer resumable process |
| EventBridge Scheduler + SQS | AWS | None; you cap consumers with reserved concurrency | Fan-out that already lives inside AWS |

None of this is worth building at low volume. If you send four hundred reminders a night, a cron job with a loop and a sleep is honest engineering, and you should stick with it until the ceiling actually bites. If your reminders are one step inside a multi-week process with human approvals, a plain queue doesn't support durable timers or resumable state and you'd be better off with Temporal, treating the send as an activity with its own retry policy. If you're on a hosted platform and the queue is the only infrastructure you'd be running, the trade-off usually favours a managed scheduler even though you give up control over the limiter's exact semantics.

The honest limitation of the Go sketch above is that its limiter is process-local. It's correct on one instance and optimistic on five — your mileage may vary depending on how evenly your load balancer spreads jobs, and I'd move the bucket into Redis before I'd trust it above two pods.

Last thing, and it's the one people skip: reminders are time-zone sensitive, and a queue drain that takes ninety minutes will push a 09:00 reminder into 10:30 for whoever happens to sort last. Either shard the fan-out by time zone and start each shard early enough to finish, or accept the drift and say so in the product copy. Pretending the queue is instantaneous is how you end up explaining to a compliance reviewer why a notice landed outside its permitted contact window.

## References

- Cron — https://en.wikipedia.org/wiki/Cron
- Exponential backoff — https://en.wikipedia.org/wiki/Exponential_backoff
- BullMQ rate limiting — https://docs.bullmq.io/guide/rate-limiting
- pg-boss — https://github.com/timgit/pg-boss
- Twilio: best practices for high throughput messaging — https://www.twilio.com/docs/messaging/guides/best-practices-for-high-throughput
- Twilio error 21211 — https://www.twilio.com/docs/api/errors/21211
- SendGrid v3 mail send — https://www.twilio.com/docs/sendgrid/api-reference/mail-send/mail-send
- Amazon SES sending quotas — https://docs.aws.amazon.com/ses/latest/dg/quotas.html
- RFC 6585, HTTP status code 429 — https://www.rfc-editor.org/rfc/rfc6585#section-4
- RFC 9110, Retry-After — https://www.rfc-editor.org/rfc/rfc9110#field.retry-after
- golang.org/x/time/rate — https://pkg.go.dev/golang.org/x/time/rate
- Inngest concurrency and throttling — https://www.inngest.com/docs/guides/concurrency
- Temporal retry policies — https://docs.temporal.io/encyclopedia/retry-policies
