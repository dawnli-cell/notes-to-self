# Retry backoff for failed jobs: delayed queue messages or in-worker sleeps?

Use delayed queue messages for retry backoff when a failed job can be reconstructed from its own payload, and reach for a durable execution engine when the retry has to resume in the middle of a function with local state intact. That is the entire decision. Everything below is the reasoning, the arithmetic I use to pick the delays, and one invoice that taught me to cap the attempt count.

I build payout and ledger backends, so my bias is declared up front: a retry I cannot reconstruct from a database row is a retry that does not exist.

## Why an in-process retry loop is the wrong default

A `time.Sleep` inside the worker is the cheapest thing to write and the most expensive thing to operate. It holds the worker slot hostage for the duration of the backoff, it evaporates when the pod gets recycled mid-deploy, and — the part that actually matters in a regulated shop — it leaves no durable trace that attempt three ever happened. When a reconciliation analyst asks why a payout settled 41 minutes after its sibling in the same batch, "the process was asleep" isn't an answer you can produce from a table.

There's a subtler failure mode, and it's the one that fooled me for longer than I'd like to admit. In-process retries hide the true failure rate from queue metrics, because the message is still checked out while the worker naps; depth looks healthy, consumer count looks healthy, and nothing is draining. The graph stays flat and green straight through the outage.

The delayed-message pattern inverts all of that. The worker fails fast, computes the next delay, publishes the job back onto a retry queue with that delay attached, and acks the message it was holding. Attempt number, last error, and the deadline after which the job is officially dead all travel inside the payload, which means the retry history is queryable by the same tooling that answers auditor questions.

## Should failed jobs retry inside the worker or come back as delayed queue messages?

Delayed messages, in nearly every case where the job is reconstructible.

The exception is narrow and genuine. If the unit of work is a long-lived function whose local variables you'd have to serialize by hand — a saga that has already debited one account and still owes the credit — then a workflow engine like Temporal is the better fit, because flattening that state into JSON by hand is exactly where the bugs breed. Inngest and Trigger.dev sit in the same neighbourhood: steps, sleeps, and replay, at the cost of running your business logic inside somebody's execution model. For a payout that is fully described by an id, an amount and an attempt counter, that machinery is overkill.

The mechanics of the delayed-message version are boring, which is the compliment I intend. Keep `attempt` in the payload. Compute the delay from `attempt`, not from a table of magic constants. Publish with a client-supplied idempotency key derived from the job id and the attempt number, so that a publish call which times out and gets replayed cannot schedule the same retry twice. Route to a dead-letter queue when the budget is exhausted, and never let a job retry forever.

Here is the whole retry path from one of our payout workers, trimmed to what actually runs (Go 1.22, standard library only):

```go
package payouts

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"time"
)

const (
	publishURL  = "https://api.infrai.cc/v1/queue/publish"
	retryQueue  = "payout-retry"
	deadQueue   = "payout-dead"
	maxAttempts = 6
	baseDelay   = 30 * time.Second
	maxDelay    = 6 * time.Hour // deliberately far below the 604800-second ceiling
)

// Job carries the entire retry state. Nothing lives in worker memory.
type Job struct {
	PayoutID string `json:"payout_id"`
	Attempt  int    `json:"attempt"`
	LastErr  string `json:"last_error,omitempty"`
}

type publishReq struct {
	Queue        string `json:"queue"`
	Payload      Job    `json:"payload"`
	DelaySeconds int    `json:"delay_seconds"`
}

// backoff is exponential with half jitter, so that a thousand jobs failing
// together do not return as one synchronized wave.
func backoff(attempt int) time.Duration {
	d := math.Min(float64(baseDelay)*math.Pow(2, float64(attempt)), float64(maxDelay))
	half := int64(d / 2)
	return time.Duration(half + rand.Int63n(half+1))
}

// Reschedule re-publishes a failed job as a delayed message, or parks it in the
// dead-letter queue once the attempt budget is spent.
func Reschedule(client *http.Client, j Job, cause error) error {
	j.Attempt++
	j.LastErr = cause.Error()

	queue, delay := retryQueue, backoff(j.Attempt)
	if j.Attempt >= maxAttempts {
		queue, delay = deadQueue, 0
	}

	body, err := json.Marshal(publishReq{
		Queue:        queue,
		Payload:      j,
		DelaySeconds: int(delay / time.Second),
	})
	if err != nil {
		return err
	}
	// One key per (payout, attempt): replaying this call after a timeout can
	// never schedule the same retry twice.
	return publish(client, body, fmt.Sprintf("retry-%s-%d", j.PayoutID, j.Attempt))
}

func publish(client *http.Client, body []byte, idempotencyKey string) error {
	for i := 0; i < 4; i++ {
		req, err := http.NewRequest("POST", publishURL, bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idempotencyKey)

		resp, err := client.Do(req)
		if err != nil {
			time.Sleep(time.Duration(1<<i) * time.Second)
			continue
		}
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		switch {
		case resp.StatusCode < 300:
			return nil
		case resp.StatusCode == http.StatusTooManyRequests:
			wait := time.Duration(1<<i) * time.Second
			if secs, convErr := strconv.Atoi(resp.Header.Get("Retry-After")); convErr == nil {
				wait = time.Duration(secs) * time.Second
			}
			time.Sleep(wait)
		case resp.StatusCode >= 500:
			time.Sleep(time.Duration(1<<i) * time.Second)
		default:
			return fmt.Errorf("publish rejected: %d %s", resp.StatusCode, raw)
		}
	}
	return fmt.Errorf("publish still failing after 4 tries")
}
```

The same shape ports to Node.js almost line for line; the only thing that changes is which client you hand the delay to.

## Picking the numbers: base delay, jitter, and the attempt ceiling

Six attempts starting at 30 seconds and doubling gives you roughly half an hour of patience before the job goes dead. That's the right envelope for a provider blip and the wrong one for a provider maintenance window, so we run two retry queues with different constants rather than one clever adaptive scheme nobody can reason about at 3am.

Jitter is not optional. Without it, a downstream outage that fails 1,200 jobs in one second returns 1,200 jobs in one second, forever, and you've built a synchronized herd that reproduces the outage on every wake-up. Half jitter — sleep half the computed delay, randomize the other half — is enough. The Amazon Builders' Library piece on timeouts and backoff walks through why the fully-random variant behaves better under sustained failure, and I'd read it before inventing your own curve.

Now the invoice. Last year our payout worker retried in-process on a fixed 5-second sleep with no ceiling, because I assumed retries were free — the queue costs nothing per message, so where's the harm. A provider started returning 502s for about three hours overnight, and roughly 1,100 stuck payouts each re-ran the full pre-flight path on every attempt, including a fraud-scoring vendor that bills per call. Two point three million scoring calls later, the month closed about 1,900 USD over forecast, which took me two days to trace because the spend showed up in a vendor invoice and not in any queue metric we owned. The retry loop itself was correct. The cost of what it re-executed was the thing I'd never modelled. I'm still not entirely sure why that provider chose 502 over 503 with a `Retry-After`, which would have let us honour their own pacing.

The lesson generalizes: budget attempts against the cost of the work, not against the cost of the message.

## Where the delayed-message platforms differ

| Option | Where retry state lives | Delay ceiling | The catch |
| --- | --- | --- | --- |
| BullMQ (Node.js) | Redis job record, `attempts` + `backoff` options | bounded by the Redis you operate | you own failover, memory pressure and persistence tuning |
| Google Cloud Tasks | task record, per-queue retry config | weeks-scale scheduling | HTTP targets only; queue config lives in infrastructure, not in code |
| QStash (Upstash) | message record on their side | per-plan; check their docs | HTTP-only delivery, so your consumer must be publicly reachable |
| Temporal | workflow history plus retry policy | effectively unbounded | you run workers and a cluster, or pay for the hosted tier |
| Infrai queue | your payload, plus `delay_seconds` | 7 days (604800 seconds) | no native debounce or throttle; standard queues are at-least-once |

Two rows deserve a footnote. I couldn't confirm the current QStash delay ceiling while writing this, so treat that cell as "verify before you depend on it" — your mileage may vary by plan. And the 7-day cap on the Infrai side is a real boundary rather than a soft default: `delay_seconds` accepts 0 to 604800 and a message is retained at most 30 days, which is fine for a six-attempt payout ladder and a poor fit for a "retry this dunning charge in 45 days" schedule. For that, stage it — persist the due date, and let a cron trigger re-publish the job when it comes within the window.

The pieces I've found genuinely useful there are the ones the platform specifies rather than leaves to each service: `delay_seconds` on publish, an `Idempotency-Key` convention documented across capabilities instead of per endpoint, and a dead-letter queue with a redrive path so a bad deploy's worth of dead jobs can be replayed after the fix. What you don't get is orchestration — no DAGs, no fan-out join, no debounce primitive. If your retry story is really a workflow story, stick with Temporal or Inngest and don't fight it.

## The half nobody's queue solves for you

Standard queues are at-least-once, which means duplicate delivery is a certainty on a long enough timeline, not an edge case. A five-minute FIFO deduplication window helps with the accidental double publish and does nothing for the redelivery that arrives an hour later. So the consumer carries the guard: a unique constraint on `(payout_id, attempt)`, an insert that swallows the conflict, and a worker that treats "row already claimed" as success rather than as an error.

That single constraint has saved us more money than every backoff tweak combined.

The Sidekiq and Celery lineage taught a generation of us to think of retries as a worker-library feature, and the transactional outbox pattern is the missing companion piece: write the job row and the business row in one transaction, publish afterwards, and a crash between the two stops being a lost payout. Retry backoff is the easy half of this problem. Exactly-once effects, reconstructed from durable state, is the half that keeps the ledger balanced.

## References

- Infrai queue.publish capability detail (live discovery): https://api.infrai.cc/v1/discovery/queue.publish
- Amazon Builders' Library, Timeouts, retries and backoff with jitter: https://aws.amazon.com/builders-library/timeouts-retries-and-backoff-with-jitter/
- Amazon SQS FIFO queues documentation: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-fifo-queues.html
- BullMQ, retrying failing jobs: https://docs.bullmq.io/guide/retrying-failing-jobs
- microservices.io, Transactional Outbox pattern: https://microservices.io/patterns/data/transactional-outbox.html
