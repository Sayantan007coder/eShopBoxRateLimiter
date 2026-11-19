package com.eshopbox.ratelimiter;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.LongSupplier;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Single-threaded behaviour, driven by a fake clock so it runs fast and gives the same result every time. */
class RateLimiterBehaviourTest {

    static final long SECOND = 1_000_000_000L;

    static Stream<Arguments> implementations() {
        BiFunction<Integer, LongSupplier, RateLimiter> fixed = FixedWindowRateLimiter::new;
        BiFunction<Integer, LongSupplier, RateLimiter> sliding = SlidingWindowRateLimiter::new;
        return Stream.of(Arguments.of("fixed", fixed), Arguments.of("sliding", sliding));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("implementations")
    void allowsExactlyNThenRejects(String name, BiFunction<Integer, LongSupplier, RateLimiter> factory) {
        AtomicLong clock = new AtomicLong(0);
        RateLimiter limiter = factory.apply(5, clock::get);
        for (int i = 0; i < 5; i++) {
            assertTrue(limiter.allowRequest(), "request " + i + " should be allowed");
        }
        assertFalse(limiter.allowRequest());
        assertFalse(limiter.allowRequest());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("implementations")
    void permitsComeBackAfterOneSecond(String name, BiFunction<Integer, LongSupplier, RateLimiter> factory) {
        AtomicLong clock = new AtomicLong(0);
        RateLimiter limiter = factory.apply(3, clock::get);
        for (int i = 0; i < 3; i++) {
            assertTrue(limiter.allowRequest());
        }
        assertFalse(limiter.allowRequest());

        clock.set(SECOND - 1);
        assertFalse(limiter.allowRequest(), "still inside the first second");

        clock.set(SECOND);
        for (int i = 0; i < 3; i++) {
            assertTrue(limiter.allowRequest());
        }
        assertFalse(limiter.allowRequest());
    }

    @Test
    void fixedWindowAllowsBurstAcrossBoundary() {
        // Known property of fixed windows: N at the end of one window plus N at the start of the next.
        AtomicLong clock = new AtomicLong(0);
        RateLimiter limiter = new FixedWindowRateLimiter(2, clock::get);
        clock.set(SECOND - 1);
        assertTrue(limiter.allowRequest());
        assertTrue(limiter.allowRequest());
        clock.set(SECOND);
        assertTrue(limiter.allowRequest());
        assertTrue(limiter.allowRequest());
        assertFalse(limiter.allowRequest());
    }

    @Test
    void slidingWindowPreventsBurstAcrossBoundary() {
        AtomicLong clock = new AtomicLong(0);
        RateLimiter limiter = new SlidingWindowRateLimiter(2, clock::get);
        clock.set(SECOND - 1);
        assertTrue(limiter.allowRequest());
        assertTrue(limiter.allowRequest());
        clock.set(SECOND);
        assertFalse(limiter.allowRequest(), "only 1ns has passed since the last two grants");
        clock.set(2 * SECOND - 1);
        assertTrue(limiter.allowRequest());
        assertTrue(limiter.allowRequest());
        assertFalse(limiter.allowRequest());
    }

    @Test
    void fixedWindowSkipsIdleWindows() {
        AtomicLong clock = new AtomicLong(0);
        RateLimiter limiter = new FixedWindowRateLimiter(1, clock::get);
        assertTrue(limiter.allowRequest());
        clock.set(10 * SECOND + 5);
        assertTrue(limiter.allowRequest());
        assertFalse(limiter.allowRequest());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("implementations")
    void handlesNanoTimeOverflow(String name, BiFunction<Integer, LongSupplier, RateLimiter> factory) {
        AtomicLong clock = new AtomicLong(Long.MAX_VALUE - SECOND / 2);
        RateLimiter limiter = factory.apply(1, clock::get);
        assertTrue(limiter.allowRequest());
        assertFalse(limiter.allowRequest());
        clock.addAndGet(SECOND); // wraps around to a negative value
        assertTrue(limiter.allowRequest());
        assertFalse(limiter.allowRequest());
    }

    @Test
    void rejectsInvalidArguments() {
        assertThrows(IllegalArgumentException.class, () -> new FixedWindowRateLimiter(0));
        assertThrows(IllegalArgumentException.class, () -> new SlidingWindowRateLimiter(-1));
        assertThrows(IllegalArgumentException.class, () -> new FixedWindowRateLimiter(1, null));
    }
}
