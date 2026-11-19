package com.eshopbox.ratelimiter;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

/**
 * Allows at most {@code N} requests per one-second window.
 *
 * <p>Time is cut into back-to-back one-second windows, counted from when the limiter
 * was created and measured with the monotonic {@link System#nanoTime()} clock.
 *
 * <h2>Thread safety</h2>
 * The state is the pair (window index, count), kept in one immutable {@link Window}
 * object inside an {@link AtomicReference}. A call to {@link #allowRequest()}:
 * <ol>
 *   <li>reads the current snapshot,</li>
 *   <li>works out the next state: a new window with count 1, or the same window with count + 1,
 *       or rejects if the window is already at {@code N},</li>
 *   <li>publishes the new state with a single {@code compareAndSet}. If another thread changed
 *       the state in the meantime, the CAS fails and the loop retries on fresh state.</li>
 * </ol>
 * Window and count change together in one atomic step, so there is no gap between
 * "check the limit" and "increment the counter", and no gap between "reset the window" and
 * "count the request". Every successful request matches exactly one successful CAS that
 * moved the count from k to k+1 with k &lt; N. So at most N requests can succeed in any window.
 * It is lock-free: a thread only retries when another thread made progress.
 */
public final class FixedWindowRateLimiter implements RateLimiter {

    private static final long WINDOW_NANOS = 1_000_000_000L;

    /** Immutable snapshot of the limiter state. */
    private record Window(long index, int count) { }

    private final int maxRequestsPerSecond;
    private final LongSupplier nanoClock;
    private final long origin;
    private final AtomicReference<Window> state;

    /** Creates a limiter that allows {@code maxRequestsPerSecond} calls per second. */
    public FixedWindowRateLimiter(int maxRequestsPerSecond) {
        this(maxRequestsPerSecond, System::nanoTime);
    }

    /**
     * Creates a limiter with an injectable clock (used by tests).
     *
     * @param maxRequestsPerSecond N, must be &gt;= 1
     * @param nanoClock            monotonic time source in nanoseconds
     */
    public FixedWindowRateLimiter(int maxRequestsPerSecond, LongSupplier nanoClock) {
        if (maxRequestsPerSecond < 1) {
            throw new IllegalArgumentException("maxRequestsPerSecond must be >= 1, was " + maxRequestsPerSecond);
        }
        if (nanoClock == null) {
            throw new IllegalArgumentException("nanoClock must not be null");
        }
        this.maxRequestsPerSecond = maxRequestsPerSecond;
        this.nanoClock = nanoClock;
        this.origin = nanoClock.getAsLong();
        this.state = new AtomicReference<>(new Window(0, 0));
    }

    @Override
    public boolean allowRequest() {
        while (true) {
            long nowWindow = currentWindowIndex();
            Window current = state.get();

            Window next;
            if (nowWindow > current.index()) {
                // A new second has started: reset and count this request in the same step.
                next = new Window(nowWindow, 1);
            } else if (current.count() < maxRequestsPerSecond) {
                // Same window (or a thread with an older clock reading): just count it.
                // If nowWindow < current.index(), this thread read the clock just before
                // another thread moved the window forward. Its request falls in the newer
                // window, which is still limited to N.
                next = new Window(current.index(), current.count() + 1);
            } else {
                return false; // limit reached for this window
            }

            if (state.compareAndSet(current, next)) {
                return true;
            }
            // Lost the race: state changed under us. Retry with a fresh snapshot.
        }
    }

    public int getMaxRequestsPerSecond() {
        return maxRequestsPerSecond;
    }

    private long currentWindowIndex() {
        // Subtraction keeps this correct even if nanoTime wraps around.
        return Math.floorDiv(nanoClock.getAsLong() - origin, WINDOW_NANOS);
    }
}
