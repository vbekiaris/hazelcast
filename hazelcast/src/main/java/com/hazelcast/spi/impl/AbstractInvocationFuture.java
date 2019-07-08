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

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.internal.util.executor.UnblockableThread;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.InternalCompletableFuture;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.annotation.Nonnull;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.hazelcast.util.ConcurrencyUtil.CALLER_RUNS;
import static com.hazelcast.util.Preconditions.isNotNull;
import static java.util.concurrent.atomic.AtomicReferenceFieldUpdater.newUpdater;
import static java.util.concurrent.locks.LockSupport.park;
import static java.util.concurrent.locks.LockSupport.parkNanos;
import static java.util.concurrent.locks.LockSupport.unpark;

/**
 * Custom implementation of {@link java.util.concurrent.CompletableFuture}.
 * <p>
 * The long term goal is that this whole class can be ripped out and replaced
 * by {@link java.util.concurrent.CompletableFuture} from the JDK. So we need
 * to be very careful with adding more functionality to this class because
 * the more we add, the more
 * difficult the replacement will be.
 * <p>
 * TODO:
 * - thread value protection
 *
 * @param <V>
 */
@SuppressWarnings("Since15")
@SuppressFBWarnings(value = "DLS_DEAD_STORE_OF_CLASS_LITERAL", justification = "Recommended way to prevent classloading bug")
public abstract class AbstractInvocationFuture<V> implements InternalCompletableFuture<V> {

    protected static final Object UNRESOLVED = new Object();

    // reduce the risk of rare disastrous classloading in first call to
    // LockSupport.park: https://bugs.openjdk.java.net/browse/JDK-8074773
    static {
        @SuppressWarnings("unused")
        Class<?> ensureLoaded = LockSupport.class;
    }

    private static final AtomicReferenceFieldUpdater<AbstractInvocationFuture, Object> STATE =
            newUpdater(AbstractInvocationFuture.class, Object.class, "state");

    protected final Executor defaultExecutor;
    protected final ILogger logger;

    /**
     * This field contain the state of the future. If the future is not
     * complete, the state can be:
     * <ol>
     * <li>{@link #UNRESOLVED}: no response is available.</li>
     * <li>Thread instance: no response is available and a thread has
     * blocked on completion (e.g. future.get)</li>
     * <li>{@link ExecutionCallback} instance: no response is available
     * and 1 {@link #andThen(ExecutionCallback)} was done using the default
     * executor</li>
     * <li>{@link WaitNode} instance: in case of multiple andThen
     * registrations or future.gets or andThen with custom Executor. </li>
     * </ol>
     * If the state is anything else, it is completed.
     * <p>
     * The reason why a single future.get or registered ExecutionCallback
     * doesn't create a WaitNode is that we don't want to cause additional
     * litter since most of our API calls are a get or a single ExecutionCallback.
     * <p>
     * The state field is replaced using a cas, so registration or setting a
     * response is an atomic operation and therefore not prone to data-races.
     * There is no need to use synchronized blocks.
     */
    // TODO can we avoid making this one protected?
    protected volatile Object state = UNRESOLVED;

    protected AbstractInvocationFuture(@Nonnull Executor defaultExecutor,
                                       @Nonnull ILogger logger) {
        this.defaultExecutor = defaultExecutor;
        this.logger = logger;
    }

    boolean compareAndSetState(Object oldState, Object newState) {
        return STATE.compareAndSet(this, oldState, newState);
    }

    protected final Object getState() {
        return state;
    }

    @Override
    public final boolean isDone() {
        return isDone(state);
    }

    private static boolean isDone(final Object state) {
        if (state == null) {
            return true;
        }

        return !(state == UNRESOLVED
                || state instanceof WaitNode
                || state instanceof Thread
                || state instanceof ExecutionCallback
                || state instanceof Waiter);
    }

    protected void onInterruptDetected() {
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return completeExceptionally(new CancellationException());
    }

    @Override
    public boolean isCancelled() {
        return isStateCancelled(state);
    }

    @Override
    public final V join() {
        try {
            // todo consider whether method ref lambda affects runtime perf / allocation rate
            return waitForResolution(this::resolveAndThrowWithJoinConvention);
        } catch (ExecutionException | InterruptedException e) {
            throw new AssertionError("Value resolution with join() conventions shouldn't throw ExecutionException or "
                    + "InterruptedException", e);
        }
    }

    @Override
    public final V get() throws InterruptedException, ExecutionException {
        // todo consider whether method ref lambda affects runtime perf / allocation rate
        return waitForResolution(this::resolveAndThrowIfException);
    }

    @Override
    public final V get(final long timeout, final TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        Object response = registerWaiter(Thread.currentThread(), null);
        if (response != UNRESOLVED) {
            return resolveAndThrowIfException(response);
        }

        long deadlineNanos = System.nanoTime() + unit.toNanos(timeout);
        boolean interrupted = false;
        try {
            long timeoutNanos = unit.toNanos(timeout);
            while (timeoutNanos > 0) {
                parkNanos(timeoutNanos);
                timeoutNanos = deadlineNanos - System.nanoTime();

                if (isDone()) {
                    return resolveAndThrowIfException(state);
                } else if (Thread.interrupted()) {
                    interrupted = true;
                    onInterruptDetected();
                }
            }
        } finally {
            restoreInterrupt(interrupted);
        }

        unregisterWaiter(Thread.currentThread());
        throw newTimeoutException(timeout, unit);
    }

    protected final V waitForResolution(ValueResolver<V> resolver)
            throws InterruptedException, ExecutionException {
        Object response = registerWaiter(Thread.currentThread(), null);
        if (response != UNRESOLVED) {
            // no registration was done since a value is available.
            return resolver.resolveAndThrowIfException(response);
        }

        boolean interrupted = false;
        try {
            for (; ; ) {
                park();
                if (isDone()) {
                    return resolver.resolveAndThrowIfException(state);
                } else if (Thread.interrupted()) {
                    interrupted = true;
                    onInterruptDetected();
                }
            }
        } finally {
            restoreInterrupt(interrupted);
        }
    }

    private static void restoreInterrupt(boolean interrupted) {
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void andThen(ExecutionCallback<V> callback) {
        andThen(callback, defaultExecutor);
    }

    @Override
    public void andThen(ExecutionCallback<V> callback, Executor executor) {
        isNotNull(callback, "callback");
        isNotNull(executor, "executor");

        Object response = registerWaiter(callback, executor);
        if (response != UNRESOLVED) {
            unblock(callback, executor);
        }
    }

    private void unblockAll(Object waiter, Executor executor) {
        while (waiter != null) {
            if (waiter instanceof Thread) {
                unpark((Thread) waiter);
                return;
            } else if (waiter instanceof ExecutionCallback) {
                unblock((ExecutionCallback) waiter, executor);
                return;
            } else if (waiter.getClass() == WaitNode.class) {
                WaitNode waitNode = (WaitNode) waiter;
                unblockAll(waitNode.waiter, waitNode.executor);
                waiter = waitNode.next;
            } else {
                unblockOtherNode(waiter, executor);
                return;
            }
        }
    }

    /**
     *
     * @param waiter    the current wait node, see javadoc of {@link #state state field}
     * @param executor  the {@link Executor} on which to execute the action associated with {@code waiter}
     */
    protected void unblockOtherNode(Object waiter, Executor executor) {
    }

    protected void unblock(final ExecutionCallback<V> callback, Executor executor) {
        try {
            executor.execute(() -> {
                try {
                    Object value = resolve(state);
                    if (value instanceof ExceptionalResult) {
                        Throwable error = unwrap((ExceptionalResult) value);
                        callback.onFailure(error);
                    } else {
                        callback.onResponse((V) value);
                    }
                } catch (Throwable cause) {
                    logger.severe("Failed asynchronous execution of execution callback: " + callback
                            + "for call " + invocationToString(), cause);
                }
            });
        } catch (RejectedExecutionException e) {
            callback.onFailure(e);
        }
    }

    // this method should not be needed; but there is a difference between client and server how it handles async throwables
    protected Throwable unwrap(ExceptionalResult result) {
        Throwable throwable = result.cause;
        if (throwable instanceof ExecutionException && throwable.getCause() != null) {
            return throwable.getCause();
        }
        return throwable;
    }

    protected abstract String invocationToString();

    protected Object resolve(Object value) {
        return value;
    }

    protected abstract V resolveAndThrowIfException(Object state) throws ExecutionException, InterruptedException;

    protected abstract V resolveAndThrowWithJoinConvention(Object state);

    protected abstract TimeoutException newTimeoutException(long timeout, TimeUnit unit);

    /**
     * Registers a waiter (thread/ExecutionCallback) that gets notified when
     * the future completes.
     *
     * @param waiter   the waiter
     * @param executor the {@link Executor} to use in case of an
     *                 {@link ExecutionCallback}.
     * @return UNRESOLVED if the registration was a success, anything else but void
     * is the response.
     */
    protected Object registerWaiter(Object waiter, Executor executor) {
        assert !(waiter instanceof UnblockableThread) : "Waiting for response on this thread is illegal";
        WaitNode waitNode = null;
        for (; ; ) {
            final Object oldState = state;
            if (isDone(oldState)) {
                return oldState;
            }

            Object newState;
            if (oldState == UNRESOLVED && (executor == null || executor == defaultExecutor)) {
                // nothing is syncing on this future, so instead of creating a WaitNode, we just try to cas the waiter
                newState = waiter;
            } else {
                // something already has been registered for syncing, so we need to create a WaitNode
                if (waitNode == null) {
                    waitNode = new WaitNode(waiter, executor);
                }
                waitNode.next = oldState;
                newState = waitNode;
            }

            if (compareAndSetState(oldState, newState)) {
                // we have successfully registered
                return UNRESOLVED;
            }
        }
    }

    void unregisterWaiter(Thread waiter) {
        WaitNode prev = null;
        Object current = state;

        while (current != null) {
            Object currentWaiter = current.getClass() == WaitNode.class ? ((WaitNode) current).waiter : current;
            Object next = current.getClass() == WaitNode.class ? ((WaitNode) current).next : null;

            if (currentWaiter == waiter) {
                // it is the item we are looking for, so lets try to remove it
                if (prev == null) {
                    // it's the first item of the stack, so we need to change the head to the next
                    Object n = next == null ? UNRESOLVED : next;
                    // if we manage to CAS we are done, else we need to restart
                    current = compareAndSetState(current, n) ? null : state;
                } else {
                    // remove the current item (this is done by letting the prev.next point to the next instead of current)
                    prev.next = next;
                    // end the loop
                    current = null;
                }
            } else {
                // it isn't the item we are looking for, so lets move on to the next
                prev = current.getClass() == WaitNode.class ? (WaitNode) current : null;
                current = next;
            }
        }
    }

    /**
     * Can be called multiple times, but only the first answer will lead to the
     * future getting triggered. All subsequent complete calls are ignored.
     *
     * @param value The type of response to offer.
     * @return <tt>true</tt> if offered response, either a final response or an
     * internal response, is set/applied, <tt>false</tt> otherwise. If <tt>false</tt>
     * is returned, that means offered response is ignored because a final response
     * is already set to this future.
     */
    @Override
    public final boolean complete(Object value) {
        return complete0(value);
    }

    public final boolean completeExceptionally(Object value) {
        return complete0(wrapThrowable(value));
    }

    private boolean complete0(Object value) {
        for (; ; ) {
            final Object oldState = state;
            if (isDone(oldState)) {
                warnIfSuspiciousDoubleCompletion(oldState, value);
                return false;
            }
            if (compareAndSetState(oldState, value)) {
                onComplete();
                unblockAll(oldState, defaultExecutor);
                return true;
            }
        }
    }

    protected void onComplete() {

    }

    // it can be that this future is already completed, e.g. when an invocation already
    // received a response, but before it cleans up itself, it receives a HazelcastInstanceNotActiveException
    private void warnIfSuspiciousDoubleCompletion(Object s0, Object s1) {
        if (s0 != s1 && !(isStateCancelled(s0)) && !(isStateCancelled(s0))) {
            logger.warning(String.format("Future.complete(Object) on completed future. "
                            + "Request: %s, current value: %s, offered value: %s",
                    invocationToString(), s0, s1), new Exception());
        }
    }

    @Override
    public String toString() {
        Object state = getState();
        if (isDone(state)) {
            return "InvocationFuture{invocation=" + invocationToString() + ", value=" + state + '}';
        } else {
            return "InvocationFuture{invocation=" + invocationToString() + ", done=false}";
        }
    }

    /**
     * If {@code resolved} is an {@link ExceptionalResult}, complete the {@code dependent}
     * exceptionally with a {@link CompletionException} that wraps the cause.
     * Used as discussed in {@link CompletionStage} javadoc regarding exceptional completion
     * of dependents.
     *
     * @param resolved  a resolved state, as returned from {@link #resolve(Object)}
     * @param dependent a dependent {@link CompletableFuture}
     * @return          {@code true} in case the dependent was completed exceptionally, otherwise {@code false}
     */
    public static boolean cascadeException(Object resolved, CompletableFuture dependent) {
        if (resolved instanceof ExceptionalResult) {
            dependent.completeExceptionally(wrapInCompletionException((((ExceptionalResult) resolved).cause)));
            return true;
        }
        return false;
    }

    public static CompletionException wrapInCompletionException(Throwable t) {
        return (t instanceof CompletionException)
                ? (CompletionException) t
                : new CompletionException(t);
    }

    /**
     * Linked nodes to record waiting {@link Thread} or {@link ExecutionCallback}
     * instances using a Treiber stack.
     * <p>
     * A waiter is something that gets triggered when a response comes in. There
     * are 2 types of waiters:
     * <ol>
     * <li>Thread: when a future.get is done.</li>
     * <li>ExecutionCallback: when a future.andThen is done</li>
     * </ol>
     * The waiter is either a Thread or an ExecutionCallback.
     * <p>
     * The {@link WaitNode} is effectively immutable. Once the WaitNode is set in
     * the 'state' field, it will not be modified. Also updating the state,
     * introduces a happens before relation so the 'next' field can be read safely.
     */
    static final class WaitNode {
        final Object waiter;
        volatile Object next;
        private final Executor executor;

        WaitNode(Object waiter, Executor executor) {
            this.waiter = waiter;
            this.executor = executor;
        }
    }

    // todo this should be an implementation detail but is currently reused in Invocation.pendingResponse
    public static final class ExceptionalResult {
        public final Throwable cause;

        public ExceptionalResult(Throwable cause) {
            this.cause = cause;
        }
    }

    interface Waiter {
        // marker interface for waiter nodes
    }

    // a WaitNode for a Function<V, R>
    protected static final class ApplyNode<V, R> implements Waiter {
        final CompletableFuture<R> future;
        final Function<V, R> function;

        public ApplyNode(CompletableFuture<R> future, Function<V, R> function) {
            this.future = future;
            this.function = function;
        }

        public void execute(Executor executor, V value) {
            if (cascadeException(value, future)) {
                return;
            }
            if (executor == null) {
                future.complete(function.apply(value));
            } else {
                executor.execute(() -> {
                    future.complete(function.apply(value));
                });
            }
        }
    }

    // a WaitNode for a Function<V, ? extends CompletionStage<R>>
    // todo next
//    protected static final class ComposeNode<V, U> {
//        final AtomicReference<? extends CompletionStage<U>> future;
//        final Function<? super V, ? extends CompletionStage<U>> function;
//
//        public ComposeNode(AtomicReference<? extends CompletionStage<U>> future,
//                           Function<? super V, ? extends CompletionStage<U>> function) {
//            this.future = future;
//            this.function = function;
//        }
//
//        public void execute(Executor executor, V value) {
//            if (cascadeException(value, future.get())) {
//                return;
//            }
//            if (executor == null) {
//                future.complete(function.apply(value));
//            } else {
//                executor.execute(() -> {
//                    future.complete(function.apply(value));
//                });
//            }
//        }
//    }

    // a WaitNode for exceptionally(Function<Throwable, V>)
    protected static final class ExceptionallyNode<R> implements Waiter {
        final CompletableFuture<R> future;
        final Function<Throwable, ? extends R> function;

        public ExceptionallyNode(CompletableFuture<R> future, Function<Throwable, ? extends R> function) {
            this.future = future;
            this.function = function;
        }

        public void execute(Object resolved) {
            if (resolved instanceof ExceptionalResult) {
                Throwable throwable = ((ExceptionalResult) resolved).cause;
                try {
                    R value = function.apply(throwable);
                    future.complete(value);
                } catch (Throwable t) {
                    future.completeExceptionally(t);
                }
            } else {
                future.complete((R) resolved);
            }
        }
    }

    // a WaitNode for a BiFunction<V, T, R>
    protected static final class HandleNode<V, T, R> implements Waiter {
        final CompletableFuture<R> future;
        final BiFunction<V, T, R> biFunction;

        public HandleNode(CompletableFuture<R> future, BiFunction<V, T, R> biFunction) {
            this.future = future;
            this.biFunction = biFunction;
        }

        public void execute(Executor executor, V value, T throwable) {
            Executor e = (executor == null) ? CALLER_RUNS : executor;
            e.execute(() -> {
                try {
                    future.complete(biFunction.apply(value, throwable));
                } catch (Throwable t) {
                    future.completeExceptionally(t);
                }
            });
        }
    }

    // a WaitNode for a BiConsumer<V, T>
    protected static final class WhenCompleteNode<V, T extends Throwable> implements Waiter {
        final CompletableFuture<V> future;
        final BiConsumer<V, T> biFunction;

        public WhenCompleteNode(@Nonnull CompletableFuture<V> future, @Nonnull BiConsumer<V, T> biFunction) {
            this.future = future;
            this.biFunction = biFunction;
        }

        public void execute(Executor executor, V value, T throwable) {
            Executor e = (executor == null) ? CALLER_RUNS : executor;
            e.execute(() -> {
                try {
                    biFunction.accept(value, throwable);
                } catch (Throwable t) {
                    completeExceptionallyWithPriority(future, throwable, t);
                    return;
                }
                complete(value, throwable);
            });
        }

        private void complete(V value, T throwable) {
            if (throwable == null) {
                future.complete(value);
            } else {
                future.completeExceptionally(throwable);
            }
        }
    }

    // a WaitNode for a Consumer<? super V>
    protected static final class AcceptNode<T> implements Waiter {
        final CompletableFuture<Void> future;
        final Consumer<T> consumer;

        public AcceptNode(@Nonnull CompletableFuture<Void> future, @Nonnull Consumer<T> consumer) {
            this.future = future;
            this.consumer = consumer;
        }

        public void execute(Executor executor, T value) {
            if (cascadeException(value, future)) {
                return;
            }
            Executor e = (executor == null) ? CALLER_RUNS : executor;
            e.execute(() -> {
                try {
                    consumer.accept(value);
                    future.complete(null);
                } catch (Throwable t) {
                    future.completeExceptionally(t);
                }
            });
        }
    }

    // a WaitNode for a Runnable
    protected static final class RunNode implements Waiter {
        final CompletableFuture<Void> future;
        final Runnable runnable;

        public RunNode(@Nonnull CompletableFuture<Void> future, @Nonnull Runnable runnable) {
            this.future = future;
            this.runnable = runnable;
        }

        public void execute(Executor executor, Object resolved) {
            if (cascadeException(resolved, future)) {
                return;
            }
            Executor e = (executor == null) ? CALLER_RUNS : executor;
            e.execute(() -> {
                try {
                    runnable.run();
                    future.complete(null);
                } catch (Throwable t) {
                    future.completeExceptionally(t);
                }
            });
        }
    }

    protected static final class ComposeNode<T, U> implements Waiter {
        final CompletableFuture<U> future;
        final Function<? super T, ? extends CompletionStage<U>> function;

        public ComposeNode(CompletableFuture<U> future, Function<? super T, ? extends CompletionStage<U>> function) {
            this.future = future;
            this.function = function;
        }

        public void execute(Executor executor, Object resolved) {
            if (cascadeException(resolved, future)) {
                return;
            }
            Executor e = (executor == null) ? CALLER_RUNS : executor;
            e.execute(() -> {
                try {
                    CompletionStage<U> r = function.apply((T) resolved);
                    r.whenComplete((v, t) -> {
                        if (t == null) {
                            future.complete(v);
                        } else {
                            future.completeExceptionally(t);
                        }
                    });
                } catch (Throwable t) {
                    future.completeExceptionally(t);
                }
            });
        }
    }

    protected static final class CombineNode<T, U, R> implements Waiter {
        final CompletableFuture<R> result;
        final CompletableFuture<? extends U> otherFuture;
        final BiFunction<? super T, ? super U, ? extends R> function;

        public CombineNode(CompletableFuture<R> future,
                           CompletableFuture<? extends U> otherFuture,
                           BiFunction<? super T, ? super U, ? extends R> function) {
            this.result = future;
            this.otherFuture = otherFuture;
            this.function = function;
        }

        public void execute(Executor executor, Object resolved) {
            if (cascadeException(resolved, result)) {
                return;
            }
            if (otherFuture.isCompletedExceptionally()) {
                otherFuture.exceptionally(t -> {
                    result.completeExceptionally(t);
                    return null;
                });
                return;
            }
            U otherValue = otherFuture.join();
            Executor e = (executor == null) ? CALLER_RUNS : executor;
            e.execute(() -> {
                try {
                    R r = function.apply((T) resolved, otherValue);
                    result.complete(r);
                } catch (Throwable t) {
                    result.completeExceptionally(t);
                }
            });
            return;
        }
    }

    private static boolean isStateCancelled(final Object state) {
        return ((state instanceof ExceptionalResult) &&
                (((ExceptionalResult) state).cause instanceof CancellationException));
    }

    private ExceptionalResult wrapThrowable(Object value) {
        if (value instanceof ExceptionalResult) {
            return (ExceptionalResult) value;
        }
        return new ExceptionalResult((Throwable) value);
    }

    protected static void completeExceptionallyWithPriority(CompletableFuture future, Throwable first,
                                                            Throwable second) {
        if (first == null) {
            future.completeExceptionally(second);
        } else {
            future.completeExceptionally(first);
        }
    }

    protected static <V> void completeFuture(CompletableFuture<V> future, V value, Throwable throwable) {
        if (throwable == null) {
            future.complete(value);
        } else {
            future.completeExceptionally(throwable);
        }
    }

    interface ValueResolver<E> {
        E resolveAndThrowIfException(Object unresolved) throws ExecutionException, InterruptedException;
    }
}
