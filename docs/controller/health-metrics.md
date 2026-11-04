# Controller health and metrics

`ControllerHealth` holds a cached aggregate status snapshot. The runtime must
explicitly publish whether it has started and has a usable scorer; this module
does not infer readiness from a listening ingestion socket or queue acceptance.

`HealthServer` is an optional, separate loopback listener with `/healthz`,
`/readyz` and `/metrics`. Health returns 200 while the listener is alive.
Readiness returns 503 until the runtime has started with a scorer, then 200
for ready or degraded state. The JSON `plans_allowed` flag is false for
memory gaps, failed restore, ingestion loss, contract mismatch or controller
errors. A working model retained after a replacement-load failure can remain
degraded without automatically suppressing plans. The planner must enforce
the flag; the diagnostics server does not execute or prevent warming itself.

Counters, gauges and exclusion/reason labels use fixed allowlists. They do
not accept file identities, handles, paths, credentials or arbitrary exception
strings. Tick durations expose cumulative sum and count, not percentiles.
The runtime owns recording these values: no ingestion, model or tick-loop
instrumentation is implicitly connected. Model-version info is deferred to
runtime integration rather than accepting unconstrained metric labels.

The server uses one request worker and an absolute two-second header deadline,
so diagnostic clients cannot create unbounded threads. It logs neither request
paths nor exception tracebacks. It is not in the foreground file-read path.

Run `tests/controller/test_health.py` in the designated remote build container.
This is the health/metrics portion of #62, not checkpoint/restore, a complete
Controller, a deployment, or production readiness evidence.
