package com.eshopbox.ratelimiter;

/**
 * A rate limiter shared by many threads that call one external channel API.
 * Implementations must be thread-safe.
 */
public interface RateLimiter {

    /**
     * Try to take one permit.
     *
     * @return {@code true} if the call may go ahead (and the permit is now used up),
     *         {@code false} if allowing it would exceed the limit.
     */
    boolean allowRequest();
}
