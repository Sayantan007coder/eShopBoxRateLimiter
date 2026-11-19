package com.eshopbox.ratelimiter;

import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;

/**
 * Stricter option: allows at most {@code N} requests in <em>any</em> one-second span,
 * not just within each fixed window.
 *
 * <p>A fixed window can let 2N calls through in a short time around a window boundary
 * (N at the end of second 0, N at the start of second 1). If the channel enforces its limit
 * on a rolling window, that burst could still get us blocked. This class prevents it.
 *
 * <h2>How it works</h2>
 * It keeps the timestamps of the last {@code N} allowed requests in a ring buffer.
 * A new request is allowed only if the oldest of those happened at least one second ago.
 * In that case the oldest slot is overwritten with "now". Each call is O(1), and memory is
 * O(N), allocated once.
 *
 * <h2>Thread safety</h2>
 * The check and the overwrite of the oldest slot must happen as one atomic step over
 * several fields (array slot + head pointer). A short critical section under a
 * {@link ReentrantLock} is the simplest way to do that correctly. The clock is read
 * <em>inside</em> the lock, so recorded timestamps never go backwards.
 */
public final class SlidingWindowRateLimiter implements RateLimiter {

    private static final long WINDOW_NANOS = 1_000_000_000L;

    private final int maxRequestsPerSecond;
    private final LongSupplier nanoClock;
    private final long[] timestamps;   // ring buffer of the last N grant times
    private final boolean[] used;      // whether a slot has ever been filled
    private int head;                  // index of the oldest slot
    private final ReentrantLock lock = new ReentrantLock();

    public SlidingWindowRateLimiter(int maxRequestsPerSecond) {
        this(maxRequestsPerSecond, System::nanoTime);
    }

    public SlidingWindowRateLimiter(int maxRequestsPerSecond, LongSupplier nanoClock) {
        if (maxRequestsPerSecond < 1) {
            throw new IllegalArgumentException("maxRequestsPerSecond must be >= 1, was " + maxRequestsPerSecond);
        }
        if (nanoClock == null) {
            throw new IllegalArgumentException("nanoClock must not be null");
        }
        this.maxRequestsPerSecond = maxRequestsPerSecond;
        this.nanoClock = nanoClock;
        this.timestamps = new long[maxRequestsPerSecond];
        this.used = new boolean[maxRequestsPerSecond];
    }

    @Override
    public boolean allowRequest() {
        lock.lock();
        try {
            long now = nanoClock.getAsLong();
            if (used[head] && now - timestamps[head] < WINDOW_NANOS) {
                return false; // N requests already granted within the last second
            }
            timestamps[head] = now;
            used[head] = true;
            head = (head + 1) % maxRequestsPerSecond;
            return true;
        } finally {
            lock.unlock();
        }
    }

    public int getMaxRequestsPerSecond() {
        return maxRequestsPerSecond;
    }
}
