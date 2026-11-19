package com.eshopbox.ratelimiter;

import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;

/** Allows up to a fixed number of requests in any one-second period. */
public final class SlidingWindowRateLimiter implements RateLimiter {

    private static final long WINDOW_NANOS = 1_000_000_000L;

    private final int maxRequestsPerSecond;
    private final LongSupplier nanoClock;
    private final long[] timestamps;
    private final boolean[] used;
    private int head;
    private final ReentrantLock lock = new ReentrantLock();

    public SlidingWindowRateLimiter(int maxRequestsPerSecond) {
        this(maxRequestsPerSecond, System::nanoTime);
    }

    public SlidingWindowRateLimiter(int maxRequestsPerSecond, LongSupplier nanoClock) {
        if (maxRequestsPerSecond < 1) {
            throw new IllegalArgumentException("Limit must be at least 1");
        }
        if (nanoClock == null) {
            throw new IllegalArgumentException("Clock is required");
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
                return false;
            }
            timestamps[head] = now;
            used[head] = true;
            head = (head + 1) % maxRequestsPerSecond;
            return true;
        } finally {
            lock.unlock();
        }
    }
}
