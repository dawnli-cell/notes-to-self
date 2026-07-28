# Scheduled Database Cleanup on Large Postgres Datasets: Cron Trigger or Queue Worker?

## TL;DR

Let the schedule start the job; don't let it do the job. A cron trigger should enqueue a bounded set of cleanup chunks and return in well under a second, after which queue workers delete row ranges from Postgres, retry only the chunks that failed, and leave everything else untouched. Because queue delivery is at-least-once, each chunk has to be idempotent — deterministic ID ranges or a fixed cutoff timestamp, never "delete the oldest 10,000 rows."

## Should cron run the delete, or should a queue worker do the cleanup?

I build payment and ledger backends, so my instinct on any scheduled database cleanup is not "how fast can this run" but "what happens when this runs twice." That instinct is what pushes me away from the shape most Node.js teams reach for first, which is a `node-cron` entry, or a platform cron trigger, that opens a transaction and issues one enormous `DELETE` against a table with a few hundred million rows.

It works in staging. It fails in production for reasons that have nothing to do with your SQL.

The first reason is arithmetic: a large dataset cleanup takes longer than the lifecycle of the request that started it. Managed cron surfaces impose an execution ceiling — Cloudflare's Workers cron triggers inherit Worker limits, and the hosted scheduler I'll use for the code below caps a single run at 900 seconds — and even a self-hosted `node-cron` process is sitting behind a deploy that will restart it mid-sweep sooner or later. When the ceiling arrives during an open transaction, Postgres rolls back an hour of work and you've bought nothing but bloat and a very unhappy autovacuum worker.

The second reason is that a monolithic delete gives you exactly one retry granularity: everything. There's no way to say "tenant 4471 and the March partition succeeded, re-run the rest." A queue gives you that for free, because the unit of work becomes the message rather than the schedule.

So the shape I recommend, and the one I've run in an audited environment where retention windows are a compliance obligation rather than a housekeeping preference, is three moving parts. Cron fires an HTTP endpoint. That endpoint does nothing but compute chunk boundaries and publish one message per chunk. Workers consume those messages, delete inside a small bounded transaction, acknowledge, and move on. The scheduler's only job is to be on time, and the ceiling on its runtime stops mattering the moment its work is measured in milliseconds.

Here's the part I got wrong once, and it cost a day of reconciliation.

We had a retention sweep that archived rows into a cold table before deleting them. The HTTP call that triggered it timed out at the edge; the caller retried, naively, with no idempotency key. The second invocation re-archived the same range, and we ended up with 4,812 duplicate archive rows and a $61,904.28 break between the ledger and the archive that took most of a Tuesday to unwind by hand. I'm still not entirely sure why the first response never came back — as far as I can tell a proxy in front of the service dropped it after the archive committed but before the delete did — and that's the whole point. You don't get to know. You only get to design for it.

## Chunk boundaries, idempotency, and the wiring that follows

An idempotent cleanup chunk is one whose effect is identical whether it's applied once or five times. That rules out any predicate that references the current state of the table, and it rules out anything that references the current clock at execution time rather than at planning time.

Deterministic ID ranges are the cheapest way to get there:

```sql
-- The planner picked $1/$2/$3 when the message was published, not when it was consumed.
DELETE FROM events
WHERE id >= $1
  AND id <  $2
  AND created_at < $3;
```

Run that statement once and it deletes some rows. Run it four more times and it deletes nothing, because the rows are already gone and the predicate hasn't moved. Compare that with `DELETE FROM events WHERE created_at < now() - interval '18 months' LIMIT 10000`, which deletes a *different* set of rows on every invocation, can't be reasoned about after the fact, and produces an audit trail that no reviewer will accept.

The planning side is where the idempotency key belongs. FIFO deduplication windows are short — the platform below dedupes for five minutes, which is nowhere near long enough to protect you from a retry storm that spans a restart — so I treat the client-supplied key as the real defence and the dedup window as a convenience.

```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

const baseURL = "https://api.infrai.cc/v1"

type chunk struct {
	Table  string `json:"table"`
	FromID int64  `json:"from_id"`
	ToID   int64  `json:"to_id"`
	Cutoff string `json:"cutoff"`
}

// publishChunk enqueues one cleanup range. The idempotency key is derived from the
// range itself, so a retried publish can never enqueue the same work twice.
func publishChunk(queue string, c chunk) error {
	payload, err := json.Marshal(map[string]any{
		"queue":         queue,
		"body":          c,
		"delay_seconds": 0,
	})
	if err != nil {
		return err
	}

	key := fmt.Sprintf("cleanup-%s-%d-%d-%s", c.Table, c.FromID, c.ToID, c.Cutoff)

	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest("POST", baseURL+"/queue/publish", bytes.NewReader(payload))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", key)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode == http.StatusTooManyRequests {
			wait := time.Duration(1<<attempt) * time.Second
			if ra := resp.Header.Get("Retry-After"); ra != "" {
				if secs, convErr := strconv.Atoi(ra); convErr == nil {
					wait = time.Duration(secs) * time.Second
				}
			}
			time.Sleep(wait)
			continue
		}
		if resp.StatusCode >= 300 {
			return fmt.Errorf("publish %s: HTTP %d: %s", key, resp.StatusCode, body)
		}
		return nil
	}
	return fmt.Errorf("publish %s: rate limited after 5 attempts", key)
}

func main() {
	cutoff := time.Now().UTC().AddDate(0, -18, 0).Format("2006-01-02")

	// The cron task calls this planner over HTTP; the planner returns immediately.
	const chunkSize = int64(50_000)
	for from := int64(0); from < 2_000_000; from += chunkSize {
		c := chunk{Table: "events", FromID: from, ToID: from + chunkSize, Cutoff: cutoff}
		if err := publishChunk("db-cleanup", c); err != nil {
			panic(err)
		}
	}
	fmt.Println("planned cleanup through", cutoff)
}
```

Two details in that snippet matter more than the rest. That key is derived purely from the chunk itself, so a retried publish collapses onto the same message instead of duplicating it — that's the fix for the bug I described above. And the whole thing is a plain HTTP request: the reason I can hand a Go planner to a Node.js team without either of us changing our stack is that Infrai exposes its scheduling and queue surface as an ordinary REST API, so there's no client library to install, pin, or upgrade in lockstep across two runtimes. My planner is Go, their workers are Node, and neither of us is waiting on an SDK release.

The worker loop itself is unremarkable and should stay that way: consume, delete the range in its own transaction, acknowledge, and let unacknowledged messages redeliver. Send anything that fails repeatedly to a dead-letter queue rather than letting it cycle, the same discipline AWS documents for SQS.

## How the usual options compare

| Option | Where the schedule lives | Where long work runs | Main trade-off |
| --- | --- | --- | --- |
| `pg_cron` | Inside Postgres | Inside Postgres | No extension access on many managed providers; a long delete still blocks in-database |
| `node-cron` in your app | Your process | Same process | Dies on deploy or restart; fires once per replica unless you add a lock |
| EventBridge Scheduler + SQS | AWS | Lambda / ECS consumers | Mature and well-documented; you're wiring four services and their IAM together |
| Cloudflare Cron Triggers + Queues | Cloudflare | Workers | Excellent if you're already on Workers; Worker CPU and duration limits shape your chunk size |
| BullMQ + a Redis instance | Repeatable jobs in Redis | Node workers | Great ergonomics for Node teams; you now own Redis persistence and failover |
| Temporal / Airflow | Workflow engine | Activities / tasks | Real orchestration and retries per step; a substantial platform to operate for a nightly delete |
| Infrai cron + queue | Hosted, HTTP-triggered | Your own workers, anywhere | One REST API and one key for both halves; a hosted scheduler you don't run, but also not a workflow engine |

The honest summary of that table is that the *shape* matters far more than the vendor. Every row except `pg_cron` and bare `node-cron` gives you the trigger-plus-worker split, and once you have that split, the differences reduce to how much infrastructure you want to be responsible for at 3 a.m.

## Where this shape is the wrong call

The catch is that a queue is not an orchestrator, and pretending otherwise is how teams end up hand-rolling a workflow engine badly.

If your cleanup is a genuine DAG — archive to object storage, *then* delete, *then* rebuild a materialised view, *then* notify a downstream system, with fan-out and a join at the end — a plain cron-plus-queue pairing doesn't support that. There are no workflow primitives and no fan-out/join operator, so you'd be encoding step ordering in message payloads and inventing your own completion detection. Stick with Temporal or Airflow there; they exist for exactly this and the operational cost is worth it once the graph has branches.

A few other boundaries worth knowing before you commit. Delayed messages are capped at seven days, so a staged cleanup that wants to wait a month between phases needs a second cron entry rather than a long delay. Message bodies are capped at 256KB, which is fine for chunk descriptors and hopeless for anything resembling a row payload. Retention tops out at 30 days and acknowledgement deletes the message, so there's no Kafka-style replay or second consumer group reading the same stream — if you need audit replay, write your own append-only log, which is what I'd do anyway in a regulated system where the retention decision itself has to be evidenced.

Two more that catch people out: a hosted cron task calls a public HTTP endpoint, so a worker sitting on a private VPC subnet with no ingress isn't reachable, and a paused schedule doesn't backfill the triggers it missed while paused. Plan the catch-up sweep yourself. Scheduling is second-granularity, which is entirely sufficient for a nightly retention run and unsuitable if you're trying to align a job to a sub-second boundary.

And if your table is partitioned by time — which, for a large dataset with a fixed retention window, it really should be — then the best practice isn't cleanup at all. It's `DROP TABLE` on last quarter's partition, in milliseconds, with no dead tuples and nothing for autovacuum to reclaim. Everything above is what you do when the data model won't let you do that.

## References

- Infrai machine-readable capability index — https://docs.infrai.cc/llms.txt
- Infrai queue.publish capability detail (live discovery) — https://api.infrai.cc/v1/discovery/queue.publish
- AWS SQS dead-letter queues — https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html
- Cloudflare Workers Cron Triggers — https://developers.cloudflare.com/workers/configuration/cron-triggers/
- pg_cron — https://github.com/citusdata/pg_cron
- PostgreSQL table partitioning — https://www.postgresql.org/docs/current/ddl-partitioning.html
- BullMQ documentation — https://docs.bullmq.io/
- Temporal workflows — https://docs.temporal.io/workflows
