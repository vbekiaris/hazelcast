/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.Operation;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Tests the {@link CompletionStage} implementation of {@link InvocationCompletionStage}.
 * Each {@code then*} method ({@code thenApply}, {@code thenAccept}, {@code thenRun}) is tested:
 * <ul>
 *     <li>across all method variants: plain, async, async with explicit executor as argument</li>
 *     <li>as a stage following a future that at the time of registration is either incomplete or completed</li>
 * </ul>
 *todo test exception rules
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class InvocationCompletionStageTest extends HazelcastTestSupport {

    private final Object returnValue = new Object();
    private final Object chainedReturnValue = new Object();

    private CountingExecutor countingExecutor;
    private HazelcastInstance local;
    private OperationServiceImpl operationService;
    private final ILogger logger = Logger.getLogger(InvocationCompletionStageTest.class);

    @Before
    public void setup() {
        local = createHazelcastInstance();
        operationService = getOperationService(local);
        countingExecutor = new CountingExecutor();
    }

    @Test
    public void thenAccept_onCompletedFuture() {
        CompletableFuture<Object> future = invokeSync();

        CompletableFuture<Void> chained = prepareThenAccept(future, false, false);

        assertTrueEventually(() -> assertTrue(future.isDone()));
        assertTrueEventually(() -> assertTrue(chained.isDone()));
    }

    @Test
    public void thenAccept_onIncompleteFuture() {
        InvocationCompletionStage<Object> future = invokeAsync(new SlowOperation(3000, "#"));

        CompletableFuture<Void> chained = prepareThenAccept(future, false, false);

        assertTrueEventually(() -> Assert.assertTrue(chained.isDone()));
    }

    @Test
    public void thenAcceptAsync_onCompletedFuture() {
        CompletableFuture<Object> future = invokeSync(new DummyOperation(null));
        CompletableFuture<Void> chained = prepareThenAccept(future, true, false);
        assertTrueEventually(() -> Assert.assertTrue(chained.isDone()));
    }

    @Test
    public void thenAcceptAsync_onIncompleteFuture() {
        InvocationCompletionStage<Object> future = invokeAsync(new SlowOperation(3000, "#"));
        CompletableFuture<Void> chained = prepareThenAccept(future, true, false);
        assertTrueEventually(() -> Assert.assertTrue(chained.isDone()));
    }

    @Test
    public void thenAcceptAsync_withExecutor_onCompletedFuture() {
        CompletableFuture<Object> future = invokeSync(new DummyOperation(null));
        CompletableFuture<Void> chained = prepareThenAccept(future, true, true);

        assertTrueEventually(() -> Assert.assertTrue(chained.isDone()));
        assertEquals(1, countingExecutor.counter.get());
    }

    @Test
    public void thenAcceptAsync_withExecutor_onIncompleteFuture() {
        InvocationCompletionStage<Object> future = invokeAsync(new SlowOperation(3000, "#"));
        CompletableFuture<Void> chained = prepareThenAccept(future, true, true);

        assertTrueEventually(() -> Assert.assertTrue(chained.isDone()));
        assertEquals(1, countingExecutor.counter.get());
    }

    @Test
    public void thenAcceptAsync_whenManyChained() {
        InvocationCompletionStage<Object> future = invokeAsync(new SlowOperation(3000, "#"));

        CompletableFuture<Void> chained1 = prepareThenAccept(future, true, true);
        CompletableFuture<Void> chained2 = prepareThenAccept(chained1, true, true);

        assertTrueEventually(() -> Assert.assertTrue(chained2.isDone()));
        assertTrue(chained1.isDone());
        assertEquals(2, countingExecutor.counter.get());
    }

    @Test
    public void thenApply_whenCompletedFuture() {
        CompletionStage<Object> future = invokeSync();
        CompletableFuture<Object> chained = future.thenApply(value -> {
            assertNull(value);
            return returnValue;
        }).toCompletableFuture();

        assertSame(returnValue, chained.join());
        assertTrue(chained.isDone());
    }

    @Test
    public void thenApply_whenIncompleteFuture() {
        CompletionStage<Object> future = invokeAsync();
        CompletableFuture<Object> chained = future.thenApply(value -> {
            assertNull(value);
            return returnValue;
        }).toCompletableFuture();

        assertSame(returnValue, chained.join());
        assertTrue(chained.isDone());
    }

    @Test
    public void thenApplyAsync_whenCompletedFuture() {
        CompletionStage<Object> future = invokeSync();
        CompletableFuture<Object> chained = future.thenApplyAsync(value -> {
            assertNull(value);
            return returnValue;
        }).toCompletableFuture();

        assertSame(returnValue, chained.join());
        assertTrue(chained.isDone());
    }

    @Test
    public void thenApplyAsync_whenIncompleteFuture() {
        CompletionStage<Object> future = invokeAsync();
        CompletableFuture<Object> chained = future.thenApplyAsync(value -> {
            assertNull(value);
            return returnValue;
        }).toCompletableFuture();

        assertSame(returnValue, chained.join());
        assertTrue(chained.isDone());
    }

    @Test
    public void thenApplyAsync_withExecutor_whenCompletedFuture() {
        CompletionStage<Object> future = invokeSync();
        CompletableFuture<Object> chained = future.thenApplyAsync(value -> {
            assertNull(value);
            return returnValue;
        }, countingExecutor).toCompletableFuture();

        assertSame(returnValue, chained.join());
        assertTrue(chained.isDone());
    }

    @Test
    public void thenApplyAsync_withExecutor_whenIncompleteFuture() {
        CompletionStage<Object> future = invokeAsync();
        CompletableFuture<Object> chained = future.thenApplyAsync(value -> {
            assertNull(value);
            return returnValue;
        }, countingExecutor).toCompletableFuture();

        assertSame(returnValue, chained.join());
        assertTrue(chained.isDone());
        assertEquals(1, countingExecutor.counter.get());
    }

    @Test
    public void thenRun_whenCompletedFuture() {
        CompletionStage<Object> future = invokeSync();
        CompletableFuture<Void> chained = future.thenRun(this::ignore).toCompletableFuture();

        assertTrue(chained.isDone());
    }

    @Test
    public void thenRun_whenIncompleteFuture() {
        CompletionStage<Object> future = invokeAsync();
        CompletableFuture<Void> chained = future.thenRun(this::ignore).toCompletableFuture();

        assertTrueEventually(() -> assertTrue(chained.isDone()));
    }

    @Test
    public void thenRunAsync_whenCompletedFuture() {
        CompletionStage<Object> future = invokeSync();
        CompletableFuture<Void> chained = future.thenRunAsync(this::ignore).toCompletableFuture();

        assertTrueEventually(() -> assertTrue(chained.isDone()));
    }

    @Test
    public void thenRunAsync_whenIncompleteFuture() {
        CompletionStage<Object> future = invokeAsync();
        CompletableFuture<Void> chained = future.thenRunAsync(this::ignore).toCompletableFuture();

        assertTrueEventually(() -> assertTrue(chained.isDone()));
    }

    @Test
    public void thenRunAsync_withExecutor_whenCompletedFuture() {
        CompletionStage<Object> future = invokeSync();
        CompletableFuture<Void> chained = future.thenRunAsync(this::ignore, countingExecutor).toCompletableFuture();

        assertTrueEventually(() -> assertTrue(chained.isDone()));
    }

    @Test
    public void thenRunAsync_withExecutor_whenIncompleteFuture() {
        CompletionStage<Object> future = invokeAsync();
        CompletableFuture<Void> chained = future.thenRunAsync(this::ignore, countingExecutor).toCompletableFuture();

        assertTrueEventually(() -> assertTrue(chained.isDone()));
        assertEquals(1, countingExecutor.counter.get());
    }

    @Test
    public void thenRunAsync_whenChained() {
        CompletionStage<Object> future = invokeAsync();
        CompletableFuture<Void> chained = future.thenRunAsync(this::ignore, countingExecutor).toCompletableFuture();

        assertTrueEventually(() -> assertTrue(chained.isDone()));
        assertEquals(1, countingExecutor.counter.get());
    }

    @Test
    public void whenComplete_whenCompletedFuture() {
        CompletionStage<Object> future = invokeSync();
        CompletableFuture<Object> chained = future.whenComplete((v, t) -> {
            assertNull(v);
            assertNull(t);
        }).toCompletableFuture();

        assertTrue(chained.isDone());
        assertNull(chained.join());
    }

    @Test
    public void whenComplete_whenIncompleteFuture() {
        CompletionStage<Object> future = invokeAsync();
        CompletableFuture<Object> chained = future.whenComplete((v, t) -> {
            assertNull(v);
            assertNull(t);
        }).toCompletableFuture();

        assertTrueEventually(() -> assertTrue(chained.isDone()));
        assertNull(chained.join());
    }

    @Test
    public void whenCompleteAsync_whenCompletedFuture() {
        CompletionStage<Object> future = invokeSync();
        CompletableFuture<Object> chained = future.whenCompleteAsync((v, t) -> {
            assertNull(v);
            assertNull(t);
        }).toCompletableFuture();

        assertTrueEventually(() -> assertTrue(chained.isDone()));
    }

    @Test
    public void whenCompleteAsync_whenIncompleteFuture() {
        CompletionStage<Object> future = invokeAsync();
        CompletableFuture<Object> chained = future.whenCompleteAsync((v, t) -> {
            assertNull(v);
            assertNull(t);
        }).toCompletableFuture();

        assertTrueEventually(() -> assertTrue(chained.isDone()));
    }

    @Test
    public void whenCompleteAsync_withExecutor_whenCompletedFuture() {
        CompletionStage<Object> future = invokeSync();
        CompletableFuture<Object> chained = future.whenCompleteAsync((v, t) -> {
            assertNull(v);
            assertNull(t);
        }, countingExecutor).toCompletableFuture();

        assertTrueEventually(() -> assertTrue(chained.isDone()));
        assertEquals(1, countingExecutor.counter.get());
    }

    @Test
    public void whenCompleteAsync_withExecutor_whenIncompleteFuture() {
        CompletionStage<Object> future = invokeAsync();
        CompletableFuture<Object> chained = future.whenCompleteAsync((v, t) -> {
            assertNull(v);
            assertNull(t);
        }, countingExecutor).toCompletableFuture();

        assertTrueEventually(() -> assertTrue(chained.isDone()));
        assertEquals(1, countingExecutor.counter.get());
    }

    @Test
    public void whenCompleteAsync_whenChained() {
        CompletionStage<Object> future = invokeAsync();
        CompletableFuture<Object> chained = future.whenCompleteAsync((v, t) -> {
            assertNull(v);
            assertNull(t);
        }, countingExecutor).toCompletableFuture();

        assertTrueEventually(() -> assertTrue(chained.isDone()));
        assertEquals(1, countingExecutor.counter.get());
    }

    @Test
    public void handle_whenCompletedFuture() {
        CompletionStage<Object> future = invokeSync();
        CompletableFuture<Object> chained = future.handle((v, t) -> {
            return chainedReturnValue;
        }).toCompletableFuture();

        assertTrue(chained.isDone());
        assertEquals(chainedReturnValue, chained.join());
    }

    @Test
    public void handle_whenIncompleteFuture() {
        CompletionStage<Object> future = invokeAsync();
        CompletableFuture<Object> chained = future.handle((v, t) -> {
            return chainedReturnValue;
        }).toCompletableFuture();

        assertTrueEventually(() -> assertTrue(chained.isDone()));
        assertEquals(chainedReturnValue, chained.join());
    }

    @Test
    public void handleAsync_whenCompletedFuture() {
        CompletionStage<Object> future = invokeSync();
        CompletableFuture<Object> chained = future.handleAsync((v, t) -> {
            return chainedReturnValue;
        }).toCompletableFuture();

        assertTrueEventually(() -> assertTrue(chained.isDone()));
        assertEquals(chainedReturnValue, chained.join());
    }

    @Test
    public void handleAsync_whenIncompleteFuture() {
        CompletionStage<Object> future = invokeAsync();
        CompletableFuture<Object> chained = future.handleAsync((v, t) -> {
            return chainedReturnValue;
        }).toCompletableFuture();

        assertTrueEventually(() -> assertTrue(chained.isDone()));
        assertEquals(chainedReturnValue, chained.join());
    }

    @Test
    public void handleAsync_withExecutor_whenCompletedFuture() {
        CompletionStage<Object> future = invokeSync();
        CompletableFuture<Object> chained = future.handleAsync((v, t) -> {
            return chainedReturnValue;
        }, countingExecutor).toCompletableFuture();

        assertTrueEventually(() -> assertTrue(chained.isDone()));
        assertEquals(1, countingExecutor.counter.get());
        assertEquals(chainedReturnValue, chained.join());
    }

    @Test
    public void handleAsync_withExecutor_whenIncompleteFuture() {
        CompletionStage<Object> future = invokeAsync();
        CompletableFuture<Object> chained = future.handleAsync((v, t) -> {
            return chainedReturnValue;
        }, countingExecutor).toCompletableFuture();

        assertTrueEventually(() -> assertTrue(chained.isDone()));
        assertEquals(1, countingExecutor.counter.get());
        assertEquals(chainedReturnValue, chained.join());
    }

    @Test
    public void handleAsync_whenChained() {
        CompletionStage<Object> future = invokeAsync();
        CompletableFuture<Object> chained = future.handleAsync((v, t) -> {
            return chainedReturnValue;
        }, countingExecutor).toCompletableFuture();

        assertTrueEventually(() -> assertTrue(chained.isDone()));
        assertEquals(1, countingExecutor.counter.get());
    }

    private CompletableFuture<Void> prepareThenAccept(CompletionStage invocationFuture,
                                boolean async,
                                boolean explicitExecutor) {

        CompletionStage<Void> chained;
        if (async) {
            if (explicitExecutor) {
                chained = invocationFuture.thenAcceptAsync(value -> {
                    logger.warning(Thread.currentThread() + " >>> " + value);
                }, countingExecutor);
            } else {
                chained = invocationFuture.thenAcceptAsync(value -> {
                    logger.warning(Thread.currentThread() + " >>> " + value);
                });
            }
        } else {
            chained = invocationFuture.thenAccept(value -> {
                logger.warning(Thread.currentThread() + " >>> " + value);
            });
        }
        return chained.toCompletableFuture();
    }

    private <R> CompletableFuture<R> invokeSync() {
        return invokeSync(new DummyOperation(null));
    }

    private <R> InvocationCompletionStage<R> invokeAsync() {
        return invokeAsync(new SlowOperation(3000, null));
    }

    private <R> InvocationCompletionStage<R> invokeAsync(Operation operation) {
        TargetInvocation invocation = new TargetInvocation(operationService.getInvocationContext(),
                operation, getAddress(local), InvocationBuilder.DEFAULT_TRY_COUNT, InvocationBuilder.DEFAULT_TRY_PAUSE_MILLIS,
                InvocationBuilder.DEFAULT_CALL_TIMEOUT, true);
        return (InvocationCompletionStage<R>) invocation.invokeAsync();
    }

    private <R> CompletableFuture<R> invokeSync(Operation operation) {
        return operationService.invokeOnTarget(null, operation, getAddress(local));
    }

    static final class CountingExecutor implements Executor {
        AtomicInteger counter = new AtomicInteger();
        AtomicBoolean completed = new AtomicBoolean();

        @Override
        public void execute(Runnable command) {
            counter.getAndIncrement();
            completed.set(true);
            command.run();
        }
    }

    @Override
    protected Config getConfig() {
        Config config = new Config();
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        return config;
    }

    private void ignore() {
    }
}
