# eShopBoxRateLimiter

A thread-safe rate limiter for API calls.

## API

```java
boolean allowRequest()
```

- Returns `true` when a request is allowed.
- Returns `false` when the limit is reached.
- The limit is set when the limiter is created.

## Limiters

### FixedWindowRateLimiter

Allows at most `N` requests in each one-second window.

- Uses an `AtomicReference` to update the window and count together.
- Uses compare-and-set (`CAS`) so concurrent calls are safe and no counter updates are lost.
- Reuses the same state safely after a window changes.

This implementation can allow `N` requests near the end of one fixed window and another `N` near the start of the next window.

### SlidingWindowRateLimiter

Allows at most `N` requests in any rolling one-second period.

- Uses a lock so the time check and request recording happen together.
- Stores the last `N` request times in a ring buffer.
- Prevents bursts across fixed window boundaries.

## Thread Safety

`FixedWindowRateLimiter` publishes each change as one atomic state update. `SlidingWindowRateLimiter` completes its check and update while holding a lock. Therefore, exactly `N` calls can return `true` in the protected period, even with many threads running concurrently.

## Notes

- The fixed-window limit applies to windows created from the limiter's monotonic start time, not calendar-second boundaries.
- `System.nanoTime()` is used instead of wall-clock time.
- If the external channel also uses a rolling one-second limit, use `SlidingWindowRateLimiter`.
- This limiter works for one Java process only. Multiple application instances need a shared distributed limiter.

Java 17 or newer and Maven are required.

## Run

Run the test suite from the project directory:

```shell
mvn test
```

Maven prints the test results to the console and writes detailed reports under
`target/surefire-reports`.

To use the limiter in Java:

```java
RateLimiter limiter = new FixedWindowRateLimiter(10);
if (limiter.allowRequest()) {
    // Make the channel API call.
}
```
