/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.impl.operationservice.impl.CompletableFutureTestUtil.CountingExecutor;
import com.hazelcast.spi.impl.operationservice.impl.CompletableFutureTestUtil.InvocationPromise;
import com.hazelcast.test.ExpectedRuntimeException;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.RootCauseMatcher;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for InvocationCompletionStage methods operating on two
 * CompletionStages.
 */
@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class BiCompletionStageTest extends HazelcastTestSupport {

    @Parameters(name = "{0},{1}")
    public static Iterable<Object[]> parameters() {
        return asList(
                new Object[][]{
                        // sync - sync
                        {new InvocationPromise(true, false), new InvocationPromise(true, false)},
                        {new InvocationPromise(true, true), new InvocationPromise(true, false)},
                        {new InvocationPromise(true, true), new InvocationPromise(true, true)},
                        {new InvocationPromise(true, false), new InvocationPromise(true, true)},
                        // async - sync
                        {new InvocationPromise(false, false), new InvocationPromise(true, false)},
                        {new InvocationPromise(false, true), new InvocationPromise(true, false)},
                        {new InvocationPromise(false, true), new InvocationPromise(true, true)},
                        {new InvocationPromise(false, false), new InvocationPromise(true, true)},
                        // async - async
                        {new InvocationPromise(false, false), new InvocationPromise(false, false)},
                        {new InvocationPromise(false, true), new InvocationPromise(false, false)},
                        {new InvocationPromise(false, true), new InvocationPromise(false, true)},
                        {new InvocationPromise(false, false), new InvocationPromise(false, true)},
                        // sync - async
                        {new InvocationPromise(true, false), new InvocationPromise(false, false)},
                        {new InvocationPromise(true, true), new InvocationPromise(false, false)},
                        {new InvocationPromise(true, true), new InvocationPromise(false, true)},
                        {new InvocationPromise(true, false), new InvocationPromise(false, true)},
                               });
    }

    @Parameter
    public InvocationPromise invocation1;

    @Parameter(1)
    public InvocationPromise invocation2;

    @Rule
    public ExpectedException expected = ExpectedException.none();

    private HazelcastInstance local;
    private CompletableFuture<Object> future1;
    private CompletableFuture<Object> future2;
    private CountingExecutor countingExecutor;

    @Before
    public void setup() {
        local = createHazelcastInstance();
        future1 = invocation1.invoke(local);
        future2 = invocation2.invoke(local);
        countingExecutor = new CountingExecutor();
    }

    @Test
    public void thenCombine() {
        CompletableFuture<Integer> combinedFuture = future1.thenCombine(future2, (v1, v2) -> {
            assertTrue(future1.isDone());
            assertTrue(future2.isDone());
            assertNull(v1);
            assertNull(v2);
            return 1;
        });
        boolean exceptionalCompletion = invocation1.throwsException || invocation2.throwsException;
        assertTrueEventually(() -> {
            assertTrue(combinedFuture.isDone());
        });
        if (exceptionalCompletion) {
            expected.expect(CompletionException.class);
            expected.expectCause(new RootCauseMatcher(ExpectedRuntimeException.class));
            combinedFuture.join();
        }
        // non-exceptional completion
        assertEquals(1, (int) combinedFuture.join());
    }

    @Test
    public void thenCombineAsync() {
        CompletableFuture<Integer> combinedFuture = future1.thenCombineAsync(future2, (v1, v2) -> {
            assertTrue(future1.isDone());
            assertTrue(future2.isDone());
            assertNull(v1);
            assertNull(v2);
            return 1;
        });
        boolean exceptionalCompletion = invocation1.throwsException || invocation2.throwsException;
        assertTrueEventually(() -> {
            assertTrue(combinedFuture.isDone());
        });
        if (exceptionalCompletion) {
            expected.expect(CompletionException.class);
            expected.expectCause(new RootCauseMatcher(ExpectedRuntimeException.class));
            combinedFuture.join();
        }
        // non-exceptional completion
        assertEquals(1, (int) combinedFuture.join());
    }

    @Test
    public void thenCombineAsync_withExecutor() {
        CompletableFuture<Integer> combinedFuture = future1.thenCombineAsync(future2, (v1, v2) -> {
            assertTrue(future1.isDone());
            assertTrue(future2.isDone());
            assertNull(v1);
            assertNull(v2);
            return 1;
        }, countingExecutor);
        boolean exceptionalCompletion = invocation1.throwsException || invocation2.throwsException;
        assertTrueEventually(() -> {
            assertTrue(combinedFuture.isDone());
        });
        if (exceptionalCompletion) {
            expected.expect(CompletionException.class);
            expected.expectCause(new RootCauseMatcher(ExpectedRuntimeException.class));
            combinedFuture.join();
        }
        // non-exceptional completion
        assertEquals(1, (int) combinedFuture.join());
        assertEquals(1, countingExecutor.counter.get());
    }
}
