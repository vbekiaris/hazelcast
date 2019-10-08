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

import com.hazelcast.core.IndeterminateOperationState;
import com.hazelcast.core.IndeterminateOperationStateException;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.DeserializingCompletableFuture;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.operationservice.impl.responses.NormalResponse;

import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.internal.util.Clock.currentTimeMillis;
import static com.hazelcast.internal.util.StringUtil.timeToString;
import static com.hazelcast.spi.impl.operationservice.impl.InvocationConstant.CALL_TIMEOUT;
import static com.hazelcast.spi.impl.operationservice.impl.InvocationConstant.HEARTBEAT_TIMEOUT;
import static com.hazelcast.spi.impl.operationservice.impl.InvocationConstant.INTERRUPTED;

/**
 * The InvocationFuture is the {@link InternalCompletableFuture} that waits on the completion
 * of an {@link Invocation}. The Invocation executes an operation.
 * <p>
 * In the past the InvocationFuture.get logic was also responsible for detecting the heartbeat for blocking operations
 * using the CONTINUE_WAIT and detecting if an operation is still running using the IsStillRunning functionality. This
 * has been removed from the future and moved into the {@link InvocationMonitor}.
 *
 * @param <E>
 */
public final class InvocationFuture<E> extends DeserializingCompletableFuture<E> {

    final Invocation invocation;
    volatile boolean interrupted;
    private final ILogger logger;

    InvocationFuture(Invocation invocation, boolean deserialize) {
        super(invocation.context.serializationService, DEFAULT_ASYNC_EXECUTOR, deserialize);
        this.logger = invocation.context.logger;
        this.invocation = invocation;
    }

    @Override
    public E get() throws InterruptedException, ExecutionException {
        try {
            final Object value = super.get();
            return returnOrThrowWithGetConventions(value);
        } catch (ExecutionException exception) {
            throw decorateExceptionalCompletionForGet(exception);
        }
    }

    @Override
    public E get(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        try {
            final Object value = super.get(timeout, unit);
            return returnOrThrowWithGetConventions(value);
        } catch (ExecutionException exception) {
            throw decorateExceptionalCompletionForGet(exception);
        }
    }

    @Override
    public E getNow(E valueIfAbsent) {
        try {
            final Object value = super.getNow(valueIfAbsent);
            return returnOrThrowWithJoinConventions(value);
        } catch (CompletionException exception) {
            throw decorateExceptionalCompletionForJoin(exception);
        }
    }

    @Override
    public E join() {
        try {
            final Object value = super.join();
            return returnOrThrowWithJoinConventions(value);
        } catch (CompletionException exception) {
            throw decorateExceptionalCompletionForJoin(exception);
        }
    }

    @Override
    public E joinInternal() {
        try {
            final Object value = super.joinInternal();
            return returnOrThrowWithJoinConventions(value);
        } catch (CompletionException exception) {
            throw decorateExceptionalCompletionForJoin(exception);
        }
    }

    protected String invocationToString() {
        return invocation.toString();
    }

    protected TimeoutException newTimeoutException(long timeout, TimeUnit unit) {
        return new TimeoutException(String.format("%s failed to complete within %d %s. %s",
                invocation.op.getClass().getSimpleName(), timeout, unit, invocation));
    }

    // public for tests
    @SuppressWarnings({"checkstyle:npathcomplexity", "checkstyle:cyclomaticcomplexity"})
    public <T> T returnOrThrowWithGetConventions(Object unresolved) throws ExecutionException, InterruptedException {
        if (unresolved == null) {
            return null;
        } else if (unresolved == INTERRUPTED) {
            throw new InterruptedException(invocation.op.getClass().getSimpleName() + " was interrupted. " + invocation);
        } else if (unresolved == CALL_TIMEOUT) {
            throw new ExecutionException(newOperationTimeoutException(false));
        } else if (unresolved == HEARTBEAT_TIMEOUT) {
            throw new ExecutionException(newOperationTimeoutException(true));
        } else if (unresolved.getClass() == Packet.class) {
            NormalResponse response = invocation.context.serializationService.toObject(unresolved);
            unresolved = response.getValue();
        } else if (unresolved.getClass() == NormalResponse.class) {
            unresolved = ((NormalResponse) unresolved).getValue();
        }

        Object value = unresolved;
        if (deserialize && value instanceof Data) {
            value = invocation.context.serializationService.toObject(value);
            if (value == null) {
                return null;
            }
        }

        return (T) value;
    }
    @SuppressWarnings({"checkstyle:npathcomplexity", "checkstyle:cyclomaticcomplexity"})
    public <T> T returnOrThrowWithJoinConventions(Object unresolved) {
        if (unresolved == null) {
            return null;
        } else if (unresolved == INTERRUPTED) {
            throw new CompletionException(
                    new InterruptedException(invocation.op.getClass().getSimpleName() + " was interrupted. " + invocation));
        } else if (unresolved == CALL_TIMEOUT) {
            throw new CompletionException(newOperationTimeoutException(false));
        } else if (unresolved == HEARTBEAT_TIMEOUT) {
            throw new CompletionException(newOperationTimeoutException(true));
        } else if (unresolved.getClass() == Packet.class) {
            // probably we won't hit this case, because Packet will be already
            // deserialized by superclass -> next block will be followed
            NormalResponse response = invocation.context.serializationService.toObject(unresolved);
            unresolved = response.getValue();
        } else if (unresolved.getClass() == NormalResponse.class) {
            unresolved = ((NormalResponse) unresolved).getValue();
        }

        Object value = unresolved;
        if (deserialize && value instanceof Data) {
            value = invocation.context.serializationService.toObject(value);
            if (value == null) {
                return null;
            }
        }

        return (T) value;
    }

    private ExecutionException decorateExceptionalCompletionForGet(ExecutionException exception) {
        if (invocation.shouldFailOnIndeterminateOperationState()
                && (exception.getCause() instanceof IndeterminateOperationState)) {
            return new ExecutionException(
                    new IndeterminateOperationStateException("indeterminate operation state", exception));
        } else {
            return exception;
        }
    }

    private CompletionException decorateExceptionalCompletionForJoin(CompletionException exception) {
        if (invocation.shouldFailOnIndeterminateOperationState()
                && (exception.getCause() instanceof IndeterminateOperationState)) {
            return new CompletionException(
                    new IndeterminateOperationStateException("indeterminate operation state", exception));
        } else {
            return exception;
        }
    }

    private OperationTimeoutException newOperationTimeoutException(boolean heartbeatTimeout) {
        StringBuilder sb = new StringBuilder();
        if (heartbeatTimeout) {
            sb.append(invocation.op.getClass().getSimpleName())
              .append(" invocation failed to complete due to operation-heartbeat-timeout. ");
            sb.append("Current time: ").append(timeToString(currentTimeMillis())).append(". ");
            sb.append("Start time: ").append(timeToString(invocation.firstInvocationTimeMillis)).append(". ");
            sb.append("Total elapsed time: ")
              .append(currentTimeMillis() - invocation.firstInvocationTimeMillis).append(" ms. ");

            long lastHeartbeatMillis = invocation.lastHeartbeatMillis;
            sb.append("Last operation heartbeat: ");
            appendHeartbeat(sb, lastHeartbeatMillis);

            long lastHeartbeatFromMemberMillis = invocation.context.invocationMonitor
                    .getLastMemberHeartbeatMillis(invocation.getTargetAddress());
            sb.append("Last operation heartbeat from member: ");
            appendHeartbeat(sb, lastHeartbeatFromMemberMillis);
        } else {
            sb.append(invocation.op.getClass().getSimpleName())
              .append(" got rejected before execution due to not starting within the operation-call-timeout of: ")
              .append(invocation.callTimeoutMillis).append(" ms. ");
            sb.append("Current time: ").append(timeToString(currentTimeMillis())).append(". ");
            sb.append("Start time: ").append(timeToString(invocation.firstInvocationTimeMillis)).append(". ");
            sb.append("Total elapsed time: ")
              .append(currentTimeMillis() - invocation.firstInvocationTimeMillis).append(" ms. ");
        }

        sb.append(invocation);
        String msg = sb.toString();
        return new OperationTimeoutException(msg);
    }

    private static void appendHeartbeat(StringBuilder sb, long lastHeartbeatMillis) {
        if (lastHeartbeatMillis == 0) {
            sb.append("never. ");
        } else {
            sb.append(timeToString(lastHeartbeatMillis)).append(". ");
        }
    }

    @Override
    protected <T> T decorateValue(Object value) {
        Object unresolved = super.decorateValue(value);
        if (unresolved instanceof Packet) {
            NormalResponse response = invocation.context.serializationService.toObject(unresolved);
            unresolved = response.getValue();
        } else if (unresolved instanceof NormalResponse) {
            unresolved = ((NormalResponse) unresolved).getValue();
        }
        if (deserialize && unresolved instanceof Data) {
            unresolved = serializationService.toObject(unresolved);
        }
        return (T) unresolved;
    }
}
