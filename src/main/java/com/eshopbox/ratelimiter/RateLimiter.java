package com.eshopbox.ratelimiter;

/** A thread-safe rate limiter. */
public interface RateLimiter {

    /** Returns true when the request is allowed. */
    boolean allowRequest();
}
