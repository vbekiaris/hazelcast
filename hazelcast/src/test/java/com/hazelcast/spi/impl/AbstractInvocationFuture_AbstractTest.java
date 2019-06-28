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

package com.hazelcast.spi.impl;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Before;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.util.ExceptionUtil.rethrowIfError;

public abstract class AbstractInvocationFuture_AbstractTest extends HazelcastTestSupport {

    protected ILogger logger;
    protected Executor executor;
    protected TestFuture future;
    protected Object value = "somevalue";

    @Before
    public void setup() {
        logger = Logger.getLogger(getClass());
        executor = Executors.newSingleThreadExecutor();
        future = new TestFuture();
    }


    class TestFuture extends AbstractInvocationFuture {
        volatile boolean interruptDetected;

        TestFuture() {
            super(AbstractInvocationFuture_AbstractTest.this.executor, AbstractInvocationFuture_AbstractTest.this.logger);
        }

        TestFuture(Executor executor, ILogger logger) {
            super(executor, logger);
        }

        @Override
        protected void onInterruptDetected() {
            interruptDetected = true;
            completeExceptionally(new InterruptedException());
        }

        @Override
        protected String invocationToString() {
            return "someinvocation";
        }

        @Override
        protected Object resolveAndThrowIfException(Object state) throws ExecutionException, InterruptedException {
            Object value = resolve(state);

            if (!(value instanceof ExceptionalResult)) {
                return value;
            } else {
                Throwable cause = ((ExceptionalResult) value).cause;
                if (cause instanceof CancellationException) {
                    throw (CancellationException) cause;
                } else if (cause instanceof ExecutionException) {
                    throw (ExecutionException) cause;
                } else if (cause instanceof InterruptedException) {
                    throw (InterruptedException) cause;
                } else if (cause instanceof Error) {
                    throw (Error) cause;
                } else {
                    throw new ExecutionException(cause);
                }
            }
        }

        @Override
        protected Object resolveAndThrowWithJoinConvention(Object state) {
            Object value = resolve(state);

            if (!(value instanceof ExceptionalResult)) {
                return value;
            } else {
                Throwable cause = ((ExceptionalResult) value).cause;
                rethrowIfError(cause);
                if (cause instanceof CompletionException) {
                    throw (CompletionException) cause;
                } else {
                    throw new CompletionException(cause);
                }
            }
        }

        @Override
        protected TimeoutException newTimeoutException(long timeout, TimeUnit unit) {
            return new TimeoutException();
        }
    }
}
