package com.eshopbox.ratelimiter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.LongSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static com.eshopbox.ratelimiter.RateLimiterBehaviourTest.SECOND;

/** Many threads calling allowRequest() at once. */
class RateLimiterConcurrencyTest {

    private static final int THREADS = 64;

    /**
     * The clock is frozen inside one second and 64 threads make 64,000 calls.
     * Exactly N must succeed: not more (safety), not fewer (no lost permits).
     * Repeated many times to raise the chance of hitting a race.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("com.eshopbox.ratelimiter.RateLimiterBehaviourTest#implementations")
    void exactlyNSucceedUnderContention(String name, BiFunction<Integer, LongSupplier, RateLimiter> factory)
            throws Exception {
        int limit = 100;
        for (int round = 0; round < 50; round++) {
            RateLimiter limiter = factory.apply(limit, () -> 0L);
            assertEquals(limit, hammer(limiter, 1_000), "round " + round);
        }
    }

    /**
     * Window reset under contention: for each of many seconds, move the clock forward and then
     * let all threads race. The first calls in a new window are the ones that reset it, so
     * this tests the reset path. Each window must give exactly N.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("com.eshopbox.ratelimiter.RateLimiterBehaviourTest#implementations")
    void exactlyNPerWindowAcrossRollovers(String name, BiFunction<Integer, LongSupplier, RateLimiter> factory)
            throws Exception {
        int limit = 37;
        AtomicLong clock = new AtomicLong(0);
        RateLimiter limiter = factory.apply(limit, clock::get);
        for (int window = 0; window < 100; window++) {
            clock.set(window * SECOND); // exactly one second since the previous window's grants
            assertEquals(limit, hammer(limiter, 200), "window " + window);
        }
    }

    /**
     * Real clock, real threads, about 2.5 seconds. Each thread also counts its own grants by the
     * window the limiter used. Since that window is internal, we check a strict but
     * observable bound instead: total grants ≤ N × (number of windows that started).
     */
    @Test
    void realClockFixedWindowRespectsLimit() throws Exception {
        int limit = 500;
        long start = System.nanoTime();
        RateLimiter limiter = new FixedWindowRateLimiter(limit);
        long deadline = start + 2_500_000_000L;
        AtomicInteger allowed = new AtomicInteger();

        runConcurrently(() -> {
            while (System.nanoTime() < deadline) {
                if (limiter.allowRequest()) {
                    allowed.incrementAndGet();
                }
            }
        });

        long elapsed = System.nanoTime() - start;
        long windowsStarted = elapsed / SECOND + 1;
        assertTrue(allowed.get() <= windowsStarted * limit,
                "allowed " + allowed.get() + " > " + windowsStarted * limit);
        assertTrue(allowed.get() >= limit * (windowsStarted - 1),
                "permits were lost: allowed " + allowed.get());
    }

    /** Runs THREADS threads that each call allowRequest() {@code callsPerThread} times; returns total successes. */
    private static int hammer(RateLimiter limiter, int callsPerThread) throws Exception {
        AtomicInteger allowed = new AtomicInteger();
        runConcurrently(() -> {
            for (int i = 0; i < callsPerThread; i++) {
                if (limiter.allowRequest()) {
                    allowed.incrementAndGet();
                }
            }
        });
        return allowed.get();
    }

    /** Starts all threads behind a latch so they really call at the same time. */
    private static void runConcurrently(Runnable task) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        try {
            CountDownLatch ready = new CountDownLatch(THREADS);
            CountDownLatch go = new CountDownLatch(1);
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < THREADS; i++) {
                futures.add(pool.submit(() -> {
                    ready.countDown();
                    go.await();
                    task.run();
                    return null;
                }));
            }
            ready.await();
            go.countDown();
            for (Future<?> f : futures) {
                f.get(60, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }
    }
}
