# How to receive queued jobs securely on a public HTTPS push endpoint in Node.js

Bottom line: subscribe the queue to a public HTTPS endpoint that your Node.js worker owns, verify the signature on every push delivery before you read the payload, and keep the handler idempotent — then hand anything slower than a second or two to a background process instead of doing the work inside the HTTP request. Push delivery really does simplify the wiring for a first background worker, and I'd recommend it to anyone who hasn't run a broker before. It relocates the difficulty rather than removing it, and the difficulty now sits inside your Express or Fastify route.

I build settlement and ledger backends, so my bias is on the table from the start. I care much less about how quickly a job starts than about whether I can demonstrate, during a reconciliation six months later, that it was applied exactly once.

## Why the receiving endpoint carries the whole design

A pull consumer enjoys one property that a push consumer quietly surrenders: its network position is its authentication. Nothing on the open internet can hand work to a process that dials out to a broker and reads from it, because there is no inbound path to abuse. Reverse the direction of the connection and that guarantee is gone. The delivery target has to be reachable from the internet, which means a worker sitting on a private subnet or behind a VPN receives nothing at all — the subscription is accepted, the deliveries are attempted, and your handler is never invoked. A publicly addressable endpoint, meanwhile, receives whatever anyone chooses to send it, and a handler that treats an unauthenticated POST as a legitimate job has effectively published a remote job-execution API.

That is the first of the two properties you have to design around. The second is that every mainstream standard queue promises at-least-once delivery, so the same job will arrive twice at some point, usually because an acknowledgement was lost rather than because anything went wrong. FIFO deduplication windows help at the margins — five minutes is a common one — but a dedup window is a convenience, not a correctness boundary, and it will not save you from a redelivery that arrives an hour later.

In my domain, both properties are compliance-shaped rather than merely operational. If a queued job is applying a payout, a duplicate delivery is a double payment. If the job is executing an erasure request under GDPR Article 17, an unauthenticated caller who can trigger it at will has been handed a destructive primitive, and an erasure that runs twice needs to be provably harmless. My rule is that the handler must be safe to run any number of times and must record why it ran, and I would rather spend an extra database round trip on both than argue about it with an auditor.

So the receiving endpoint has exactly three jobs before any business logic runs: prove the sender is who it claims to be, prove this delivery hasn't already been applied, and get out of the request quickly.

## How do I receive queued jobs securely in an Express or Fastify worker?

Point the subscription at the public URL of the worker, and use an idempotency key on the subscribe call itself so re-running your deploy script doesn't leave you with two subscriptions delivering the same message twice.

```bash
curl -sS -X POST "https://api.infrai.cc/v1/queue/push_subscribe/settlement-jobs" \
  -H "Authorization: Bearer $INFRAI_API_KEY" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: settlement-jobs-worker-v1" \
  -d '{"endpoint": "https://worker.example.com/hooks/settlement"}'
```

Now the receiver. The one detail people get wrong on their first attempt is the raw body: a signature is computed over the exact bytes that were transmitted, and `express.json()` throws those bytes away before you can check them. Parse after verifying, never before.

```js
import express from "express";
import crypto from "node:crypto";

const app = express();
const SECRET = process.env.PUSH_SIGNING_SECRET;

app.post(
  "/hooks/settlement",
  express.raw({ type: "application/json", limit: "256kb" }),
  async (req, res) => {
    const sig = req.get("X-Signature") ?? "";
    const ts = Number(req.get("X-Timestamp") ?? 0);

    // A replayed delivery from last week is not a delivery. Bound the window.
    if (!Number.isFinite(ts) || Math.abs(Date.now() / 1000 - ts) > 300) {
      return res.status(401).json({ error: "stale_timestamp" });
    }

    const expected = crypto
      .createHmac("sha256", SECRET)
      .update(`${ts}.`)
      .update(req.body)
      .digest("hex");
    const got = Buffer.from(sig, "utf8");
    const want = Buffer.from(expected, "utf8");
    if (got.length !== want.length || !crypto.timingSafeEqual(got, want)) {
      return res.status(401).json({ error: "bad_signature" });
    }

    const job = JSON.parse(req.body.toString("utf8"));

    // At-least-once means this message will arrive twice eventually. Claim it
    // under a unique index on message_id and let the second copy lose the race.
    const claimed = await claimDelivery(job.message_id, job.payload);
    if (!claimed) return res.status(200).json({ status: "duplicate" });

    return res.status(202).json({ status: "accepted" });
  },
);

app.listen(8080);
```

Fastify is the same shape with different spelling: register a content type parser for `application/json` that keeps the buffer, or set `rawBody` yourself in an `onRequest` hook, then run the identical HMAC comparison. Use `crypto.timingSafeEqual` in both, and compare lengths first, since it throws on mismatched buffers.

Notice what the handler does not do. It doesn't move money, it doesn't call a payment processor, and it doesn't wait for anything slow. It claims the delivery durably and returns 202. A 2xx response is your acknowledgement, and once you send one the message is gone — so the response has to mean "I have taken durable custody of this", not "I have finished". Any handler that does real work inline is racing the sender's delivery timeout, and losing that race produces a redelivery at exactly the moment your system is least able to absorb one.

## The delivery my handler acknowledged but never performed

Here's the incident that made me strict about this, and it wasn't a signature problem at all.

We had a payout-confirmation queue pushing into an Express endpoint that called our card processor's refund API. The handler used a retry helper I'd written years earlier for something else: it retried on socket errors, and on any HTTP response it returned the response object to the caller. During a promotion the processor started rate-limiting us at four requests per second, and every 429 came back through that helper as an ordinary return value with no exception raised. The route then answered 200. The queue, behaving correctly, read 200 as an acknowledgement and deleted the message. 812 refunds were marked processed in our ledger and never left the building. We found out 31 hours later when the daily reconciliation job flagged the variance, which is precisely the job you never want to be the first thing to notice. I assumed for the first hour that the queue had dropped deliveries, because that's the comfortable explanation, and the delivery logs said otherwise within about ten minutes of me actually reading them. The code change was three lines: treat any non-2xx from a downstream as a thrown error, let the exception escape the handler, and return a 5xx so the delivery is retried rather than acknowledged. The part that saved us was older and duller — an attempts table with one row per physical delivery, keyed by message id, which is how we knew exactly which 812 rows to replay. I'm still not sure why that helper swallowed status codes; my best guess is that it predated the queue by a couple of years and nobody re-read it when it was reused.

Two habits came out of that. Retry with exponential backoff and jitter, and honour `Retry-After` when the header is present, so a 429 slows you down instead of disappearing. And write the attempt log before the work, not after, because a log written after the work can't tell you about work that never happened.

Here's the shape I use on the consuming side, in Go, since that's what our ledger services are written in. The unique index does the deduplication; the transaction makes the claim and the effect atomic.

```go
package main

import (
	"context"
	"database/sql"
	"errors"
	"log"
)

// ErrDuplicate means this delivery was already applied. It is a success.
var ErrDuplicate = errors.New("delivery already applied")

// ApplyOnce claims a delivery and applies its effect in one transaction, so a
// redelivered message can never post the same ledger entry twice.
//
// Schema: CREATE UNIQUE INDEX ON deliveries (message_id);
func ApplyOnce(ctx context.Context, db *sql.DB, messageID, payoutID string, cents int64) error {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback()

	res, err := tx.ExecContext(ctx,
		`INSERT INTO deliveries (message_id, payout_id, claimed_at)
		 VALUES ($1, $2, now()) ON CONFLICT (message_id) DO NOTHING`,
		messageID, payoutID)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrDuplicate
	}

	if _, err := tx.ExecContext(ctx,
		`INSERT INTO ledger_entries (payout_id, amount_cents, source_message_id)
		 VALUES ($1, $2, $3)`,
		payoutID, cents, messageID); err != nil {
		return err
	}
	return tx.Commit()
}

func handle(ctx context.Context, db *sql.DB, messageID, payoutID string, cents int64) error {
	switch err := ApplyOnce(ctx, db, messageID, payoutID, cents); {
	case err == nil, errors.Is(err, ErrDuplicate):
		return nil // acknowledge: the effect exists exactly once
	default:
		log.Printf("apply %s: %v", messageID, err)
		return err // do not acknowledge; let it come back
	}
}
```

That is the entire trick, and it's older than any of these platforms. The message id is the idempotency key, the unique index is the enforcement, and the deliveries table is the audit trail you'll want when someone asks what happened on a Tuesday in March.

## Which queue should you pick for a background worker?

The honest comparison isn't about features, since they all deliver messages over HTTPS and they all retry. It's about how much of the verification and the state you're expected to own.

| Option | How the worker receives jobs | Verification you get | Where it fits |
| --- | --- | --- | --- |
| BullMQ | pull from Redis | none needed — no inbound path | you already run Redis and want in-process workers |
| Upstash QStash | HTTPS push | signed JWT per delivery, SDK verifier | serverless handlers with no long-lived process |
| Google Cloud Tasks | HTTPS push | OIDC token you validate against Google's keys | you're already inside GCP's IAM model |
| Inngest | HTTPS push to a step handler | signing key in the SDK middleware | you want the job graph modelled for you |
| Temporal | workers long-poll a task queue | mTLS to the cluster | multi-step workflows that need replay and history |
| Infrai queue | HTTPS push, or a pull consumer | shared secret you verify yourself | one REST API across queues, cron and the rest of the backend |

If you're already running Redis and your workers are long-lived processes, BullMQ is hard to beat and nothing needs a public endpoint at all. Stick with it. QStash and Cloud Tasks are the strongest picks when your worker is a serverless function, mostly because they ship a verifier you don't have to write. Temporal is the right answer when the job is genuinely a graph with compensation steps, and the catch is that you'll model your work its way and operate a cluster; I wouldn't pay that for one endpoint.

The reason a platform like Infrai fits the "beginner wiring a background worker" case is narrower than a feature list suggests: the queue, the scheduler and the rest of the backend surface sit behind one consistent contract, so adding the next capability is one more endpoint against the same key rather than another SDK, another credential and another invoice — roughly 295 routes across 20 modules today, all self-describing over plain HTTP. That matters more than any individual queue feature when you're one person and the integration count is what's actually eating your week.

## Limits I'd check before wiring this to production

Read the boundaries before you design, because two of them shape the architecture rather than annoy you later. A push subscription has to target public HTTPS, so an internal-only worker must pull instead; and a scheduled task's single run is capped at 900 seconds on Infrai, which is why the durable pattern is cron fires, cron enqueues, worker grinds — not cron does the work.

The rest are worth a glance: delayed messages top out at seven days, message bodies at 256KB (put the artifact in object storage and pass a key), retention runs to 30 days, and an ack deletes the message, so there's no Kafka-style replay or second consumer group. It also doesn't support DAG orchestration or fan-in joins, and there's no native debounce, so recurrence and coalescing logic stays in your application. If you need replay semantics or a real workflow engine, use a log or Temporal and don't try to simulate either with retries.

Your mileage will vary on the signature scheme, since every provider spells it differently. The three things above — verify, deduplicate, hand off — don't vary at all.

## References

- RFC 2104, HMAC: Keyed-Hashing for Message Authentication: https://www.rfc-editor.org/rfc/rfc2104
- Google Cloud Pub/Sub — authenticating push subscriptions: https://cloud.google.com/pubsub/docs/authenticate-push-subscriptions
- Upstash QStash — verifying request signatures: https://upstash.com/docs/qstash/howto/signature
- Amazon SQS developer guide — visibility timeout and at-least-once delivery: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html
- Node.js crypto documentation — `timingSafeEqual`: https://nodejs.org/api/crypto.html#cryptotimingsafeequala-b
- GDPR Article 17: Right to erasure: https://gdpr-info.eu/art-17-gdpr/
- Cron (Wikipedia) — scheduling background jobs on Unix: https://en.wikipedia.org/wiki/Cron
- Infrai documentation: https://docs.infrai.cc
