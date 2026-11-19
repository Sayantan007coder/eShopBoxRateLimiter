package com.eshopbox.ratelimiter;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

/** Allows up to a fixed number of requests per one-second window. */
public final class FixedWindowRateLimiter implements RateLimiter {

    private static final long WINDOW_NANOS = 1_000_000_000L;

    private record Window(long index, int count) { }

    private final int maxRequestsPerSecond;
    private final LongSupplier nanoClock;
    private final long origin;
    private final AtomicReference<Window> state;

    public FixedWindowRateLimiter(int maxRequestsPerSecond) {
        this(maxRequestsPerSecond, System::nanoTime);
    }

    public FixedWindowRateLimiter(int maxRequestsPerSecond, LongSupplier nanoClock) {
        if (maxRequestsPerSecond < 1) {
            throw new IllegalArgumentException("Limit must be at least 1");
        }
        if (nanoClock == null) {
            throw new IllegalArgumentException("Clock is required");
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
                next = new Window(nowWindow, 1);
            } else if (current.count() < maxRequestsPerSecond) {
                next = new Window(current.index(), current.count() + 1);
            } else {
                return false;
            }

            if (state.compareAndSet(current, next)) {
                return true;
            }
        }
    }

    private long currentWindowIndex() {
        return Math.floorDiv(nanoClock.getAsLong() - origin, WINDOW_NANOS);
    }
}
