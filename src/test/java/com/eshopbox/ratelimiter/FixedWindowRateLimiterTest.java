package com.eshopbox.ratelimiter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class FixedWindowRateLimiterTest {

    private static final long ONE_SECOND_NANOS = 1_000_000_000L;

    @Test
    void allowsNoMoreThanConfiguredLimitInEachWindow() {
        AtomicLong clock = new AtomicLong();
        FixedWindowRateLimiter limiter = new FixedWindowRateLimiter(2, clock::get);

        assertTrue(limiter.allowRequest());
        assertTrue(limiter.allowRequest());
        assertFalse(limiter.allowRequest());

        clock.set(ONE_SECOND_NANOS);

        assertTrue(limiter.allowRequest());
        assertTrue(limiter.allowRequest());
        assertFalse(limiter.allowRequest());
    }

    @Test
    void printsEachCallResultToConsole() {
        FixedWindowRateLimiter limiter = new FixedWindowRateLimiter(2, () -> 0L);
        String[] ordinals = {"1st", "2nd", "3rd", "4th"};
        String[] expectedResults = {"allowed", "allowed", "failed", "failed"};

        for (int i = 0; i < ordinals.length; i++) {
            boolean allowed = limiter.allowRequest();
            String result = allowed ? "allowed" : "failed";
            System.out.println(ordinals[i] + " call: " + result);
            assertEquals(expectedResults[i], result);
        }
    }

    @Test
    void concurrentCallsNeverExceedConfiguredLimit() throws Exception {
        int limit = 7;
        int threadCount = 24;
        int callsPerThread = 50;
        FixedWindowRateLimiter limiter = new FixedWindowRateLimiter(limit, () -> 0L);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch ready = new CountDownLatch(threadCount);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger allowed = new AtomicInteger();
        List<Future<?>> tasks = new ArrayList<>();

        try {
            for (int i = 0; i < threadCount; i++) {
                tasks.add(executor.submit(() -> {
                    ready.countDown();
                    if (!start.await(5, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("Timed out waiting to start");
                    }
                    for (int call = 0; call < callsPerThread; call++) {
                        if (limiter.allowRequest()) {
                            allowed.incrementAndGet();
                        }
                    }
                    return null;
                }));
            }

            assertTrue(ready.await(5, TimeUnit.SECONDS));
            start.countDown();
            for (Future<?> task : tasks) {
                task.get(5, TimeUnit.SECONDS);
            }

            assertEquals(limit, allowed.get());
            assertFalse(limiter.allowRequest());
        } finally {
            executor.shutdownNow();
        }
    }
}
