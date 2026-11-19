# eShopBoxRateLimiter

Thread-safe rate limiter for syncing inventory with external sales-channel APIs.

Each channel allows at most **N calls per second**. Many application threads call `allowRequest()` at the same time, and we need one shared limiter that never lets more than `N` calls succeed in a single second.

## API

```java
boolean allowRequest()
```

- Returns `true` if the call may go ahead (still under the limit for the current second).
- Returns `false` if allowing it would exceed the limit.
- `N` is configurable at construction.

## Implementations

### `FixedWindowRateLimiter` (recommended — default choice)
Partitions time into back-to-back one-second windows (monotonic `System.nanoTime()` from construction) and allows at most `N` grants per window.

- Lock-free: the whole state `(windowIndex, count)` lives in a single `AtomicReference<Window>`.
- `allowRequest()` computes the next state and publishes it with one `compareAndSet`. If another thread moved the state first, the CAS fails and the loop retries on fresh state.
- Because the window index **and** count change in one atomic step, there is no gap between "check the limit" and "increment the counter", and no gap between "reset the window" and "count the request".
- At most `N` successful grants per window — every success matches exactly one CAS that moved count `k → k+1` with `k < N`.

Trade-off: like any fixed window, it can briefly allow `2N` calls across a boundary (N at the end of second *t*, N at the start of second *t+1*) if the channel enforces its limit on a rolling clock rather than aligned windows.

### `SlidingWindowRateLimiter` (stricter)
Allows at most `N` grants in **any** one-second span, preventing the boundary burst above.

- Keeps the timestamps of the last `N` grants in a ring buffer; a new grant is allowed only if the oldest recorded grant is at least one second old.
- The check-and-overwrite touches several fields atomically, so it runs inside a short `ReentrantLock` critical section.
- O(1) time per call, O(N) memory allocated once.

## Thread-safety summary

The correctness argument does not rely on timing:

1. The grant decision and the state update are a single atomic step (one `CAS`, or one lock acquisition).
2. A grant is counted exactly once — matched to the atomic step that published it.
3. A window's count can only reach `N`, because the `N+1`-th grant sees `count == N` and is rejected before any CAS/lock can publish it.
4. Window rollover is also atomic: resetting the window and counting the first request of the new window happen in the same step, so two threads cannot both "reset" and both count into the same new window beyond `N`.

So no matter how many threads call `allowRequest()` concurrently, at most `N` calls return `true` in any second.

## Build & test

Requires Java 17+ and Maven.

```bash
mvn test
```

Tests use a controllable fake clock so concurrency assertions are exact and deterministic, plus one real-clock test that runs for ~2.5 seconds with 32 threads. Concurrency tests are repeated many times (50 rounds) to maximise the chance of exposing a race.

## Things I was unsure about

- **Fixed vs sliding window semantics.** The problem says "no more than N calls per second", which I read as a fixed one-second window aligned to the limiter's start. If the channel instead measures on a rolling wall-clock second, `SlidingWindowRateLimiter` is the safer choice — it never allows the `2N` boundary burst, at the cost of a lock and O(N) memory.
- **Clock choice.** I used `System.nanoTime()` (monotonic) rather than `System.currentTimeMillis()` so the limiter is immune to wall-clock adjustments (NTP steps, leap seconds). The downside is the window is relative to when the limiter was created, not to the calendar second. For an inventory-sync worker that's usually fine; if the channel resets its quota on calendar-second boundaries, a clock aligned to wall time would be needed.
- **NanoTime wraparound.** `nanoTime()` can wrap around after ~292 years. I used `Math.floorDiv(now - origin, WINDOW_NANOS)`, which stays correct across a wraparound as long as no single window spans the wrap (true here), and tested it.
- **Blocking vs non-blocking.** `allowRequest()` returns immediately with a boolean; it does not block or retry internally. Callers that need to wait can loop with a small sleep/backoff outside the limiter.
- **Distributed rate limiting.** This is a single-JVM, in-memory limiter. If inventory sync runs on multiple instances behind a load balancer, each instance would have its own counter and the channel could see more than N calls/second in total. A distributed solution (e.g. Redis-backed Lua script) would be needed there — out of scope for this test.
