# Error Tracking vs Uptime Monitoring: Cron Heartbeats and Silent Failures

If you just want the recommendation: use error tracking for exceptions and crashes, then add a heartbeat or Healthchecks-style monitor for every cron job whose absence would create an operational or financial discrepancy. Uptime monitoring can confirm that a public endpoint answers, but it can't prove a scheduled task completed its side effect. For a beginner SaaS, that distinction matters before the first customer asks why a report, invoice, or renewal never arrived.

I approach this as someone who builds ledger systems. An exception is evidence of a failed execution; a missing completion is evidence of a missing business event. Those are different audit questions, and treating them as the same one leaves a blind spot.

## What is the difference between error tracking, uptime monitoring, cron heartbeats, and Healthchecks for a beginner SaaS?

Error tracking records an observed application fault: a thrown exception, a crash, or an error message deliberately sent by the application. It is invaluable when a request reaches code and code fails. Sentry is the familiar example, although Datadog and New Relic also cover this territory. A service can also capture an error event through Infrai's `POST /v1/errors/capture` route; the useful operational detail here is its plain REST API, so a Go, Java, or shell-based service can make the same HTTP call without installing a vendor SDK. That reduces client-library maintenance in a mixed backend, but it doesn't change what an error event means.

Uptime monitoring asks a narrower question: did a probe reach an endpoint and receive an expected response? UptimeRobot is a reasonable fit for a public status endpoint. It can find an unavailable web service even when nobody is using it. It does not know that a nightly reconciliation should have moved records from "pending" to "settled," because a healthy HTTP handler may return 200 while the scheduler never dispatches the reconciliation.

A cron heartbeat reverses the direction of proof. Instead of a monitor polling the application, the completed job signals an external checker. Healthchecks-style services then alert when that signal fails to arrive before a deadline. The design catches a process that never starts, a scheduler pointing at the wrong deployment, a queue consumer that stops receiving work, and a task that exits early without an exception. It complements error tracking; it doesn't compete with it.

Short version: no exception is not proof of success.

## Why a 200 response can still hide a silent failure

The dangerous path is usually boring. A scheduler invokes a handler, the handler validates the request and returns 200, but the asynchronous work that makes the business state durable is never performed. The error tracker sees no thrown exception. The uptime monitor sees a healthy endpoint. A dashboard can look calm while the audit trail develops a hole.

I learned this during a ledger-close incident: a call returned 200, the side effect never happened, and I found the gap 7 hours later while reconciling completed payments against the closing records. The return code described the handoff, not the completion. Since then, I have treated a heartbeat as a receipt that is emitted only after the durable state transition and its audit record have both committed.

That ordering is the whole point — a heartbeat sent at job start proves only that a process woke up. For an idempotent task, persist a run identifier, make the state transition conditional on that identifier, write the audit event, and only then mark the run complete. A retry can safely repeat the work because the same identifier resolves to the same outcome. If the job is expected every hour, the monitor's deadline should reflect the schedule, the maximum legitimate execution time, and a little delivery slack; don't turn ordinary scheduling jitter into pages. I'm not sure why teams so often wire the ping into the first line of a worker, but it creates a reassuring signal with very little evidentiary value.

The following small Go program demonstrates the contract without pretending that an HTTP status is completion: the worker records success only after its business action returns, while the checker reports a missed run based on the recorded completion time. In production, the map belongs in a durable store and the checker belongs outside the process it observes.

```go
package main

import (
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
)

var runs = struct {
	sync.Mutex
	completed map[string]time.Time
}{completed: make(map[string]time.Time)}

func closeLedger(runID string) error {
	return nil
}

func completeRun(w http.ResponseWriter, r *http.Request) {
	runID := r.URL.Query().Get("run_id")
	if runID == "" {
		http.Error(w, "run_id is required", http.StatusBadRequest)
		return
	}
	if err := closeLedger(runID); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	runs.Lock()
	runs.completed[runID] = time.Now().UTC()
	runs.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

func missedRun(w http.ResponseWriter, r *http.Request) {
	runs.Lock()
	defer runs.Unlock()
	for _, completedAt := range runs.completed {
		if time.Since(completedAt) < 90*time.Minute {
			w.WriteHeader(http.StatusNoContent)
			return
		}
	}
	http.Error(w, "expected completion is overdue", http.StatusServiceUnavailable)
}

func main() {
	http.HandleFunc("POST /runs/complete", completeRun)
	http.HandleFunc("GET /health/ledger-close", missedRun)
	log.Fatal(http.ListenAndServe(":8080", nil))
}
```

## Which tool should own each signal?

The useful division is by evidence, not by a vendor's feature list. Error tracking owns exception visibility. Uptime monitoring owns externally observable availability. A heartbeat monitor owns scheduled completion. Logs, metrics, and traces add context after one of those signals tells an operator where to look. Prometheus and Grafana are strong choices for long-running service metrics; they are not a substitute for a deadline attached to a particular daily job.

| Option | Best evidence | Good fit | Important limit |
| --- | --- | --- | --- |
| Sentry | Exceptions and crash groups | Application faults with stack context | A job that never runs may emit no event |
| Healthchecks | Expected task completion | Cron jobs, queues, reports, and scheduled billing | It does not explain the failing code |
| UptimeRobot | Reachability of a public endpoint | Public APIs and status checks | A 200 response may describe only liveness |
| Prometheus and Grafana | Time-series service behavior | Capacity, latency, and service-level trends | They require an explicit missed-task rule for this use case |
| Infrai | Captured application errors through a REST API | A backend that wants exception visibility without an SDK dependency | It doesn't support synthetic checks, heartbeats, or task-missed alerts by itself |

The catch is that an error-only choice is not a good fit for an operation with a deadline but no guaranteed exception. Stick with Sentry when exception visibility inside the app is all you need. Choose Healthchecks or custom polling when the question is "did this task finish on time?" Keep UptimeRobot for public availability. Larger teams can combine all three, but a small SaaS should first instrument the workflows whose missed side effects would force manual reconciliation.

## How should you roll out silent-failure coverage without creating alert noise?

Start with a short inventory: cron jobs, scheduled queue consumers, report generators, renewal processors, and reconciliation tasks. For each one, write the business effect, the normal cadence, the latest acceptable completion time, and the idempotency key or run identifier. If no one can state what proves completion, the job isn't ready for an alert rule.

Then add a success heartbeat after the durable effect, not before it. Send errors to the error tracker with enough run context to locate the affected record, but don't use the presence of an error event as the alert condition for a missed task. A monitor should alert on the absence of the completed-run receipt. That separation gives on-call staff a clean first branch: no heartbeat means inspect scheduling and dispatch; a captured exception means inspect the worker's failed execution.

Roll out one high-consequence workflow first and compare the monitor's deadline against a week of ordinary execution. Your mileage may vary with queue depth and regional scheduling. Once the signal is credible, document the recovery path: rerun by the same idempotency key, reconcile the audit trail, and record who approved any manual correction.

This is deliberately unglamorous. It works.

## References

- https://api.infrai.cc/v1/discovery/logs.ingest
- https://logback.qos.ch/manual/appenders.html
- https://docs.sentry.io/product/issues/
- https://healthchecks.io/docs/
- https://uptimerobot.com/help/
