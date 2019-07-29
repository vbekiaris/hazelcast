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

import com.hazelcast.spi.impl.AbstractInvocationFuture;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.hazelcast.util.ConcurrencyUtil.CALLER_RUNS;
import static java.util.Objects.requireNonNull;

/**
 * Exceptions behaviour:
 * <ul>
 *     <li>In JDK CompletableFuture, futures completed exceptionally pass the exact exception to the callback,
 *     wrap in {@code CompletionException} in {@code join()}, taking care not to re-wrap exceptions:<br>
 *         <pre>{@code
 *         future.completeExceptionally(new OperationTimeoutException())
 *               .whenCompleteAsync((val, throwable) -> System.out.println(throwable)) // prints OperationTimeoutException
 *               .join(); // throws CompletionException(OperationTimeoutException)
 *
 *         future.completeExceptionally(new CompletionException(new OperationTimeoutException()))
 *  *               .whenCompleteAsync((val, throwable) -> System.out.println(throwable)) // prints CompletionException
 *  *               .join(); // throws CompletionException(OperationTimeoutException)
 *
 *         }
 *         </pre>
 *
 *     </li>
 * </ul>
 * @param <V>
 */
// todo check type arguments
// todo tests for nullability of arguments
// todo tests for exceptions thrown from user customizations
// todo deduplication of WaitNodes code (possible to extract interface?)
// todo pull CompletionStage implementation to AbstractInvocationFuture? or compose to other class for reuse in client-side futures?
public class InvocationCompletionStage<V> extends InvocationFuture<V> implements CompletionStage<V> {

    public InvocationCompletionStage(Invocation invocation, boolean deserialize) {
        super(invocation, deserialize);
    }

    // thenAccept* implementation
    @Override
    public CompletionStage<Void> thenAccept(@Nonnull Consumer<? super V> action) {
        return unblock(action, null);
    }

    @Override
    public CompletionStage<Void> thenAcceptAsync(@Nonnull Consumer<? super V> action) {
        return unblock(action, defaultExecutor);
    }

    @Override
    public CompletionStage<Void> thenAcceptAsync(@Nonnull Consumer<? super V> action, Executor executor) {
        return unblock(action, executor);
    }

    protected CompletionStage<Void> unblock(@Nonnull final Consumer<? super V> consumer, Executor executor) {
        requireNonNull(consumer);
        final Object value = resolve(state);
        final CompletableFuture<Void> result = newCompletableFuture();
        if (value != UNRESOLVED) {
            if (cascadeException(value, result)) {
                return result;
            }
            Executor e = (executor == null) ? CALLER_RUNS : executor;
            e.execute(() -> {
                try {
                    consumer.accept((V) value);
                    result.complete(null);
                } catch (Throwable t) {
                    result.completeExceptionally(t);
                }
            });
            return result;
        } else {
            registerWaiter(new AcceptNode(result, consumer), executor);
            return result;
        }
    }

    @Override
    protected void unblockOtherNode(Object waiter, Executor executor) {
        Object value = resolve(state);
        if (waiter instanceof AcceptNode) {
            AcceptNode acceptNode = (AcceptNode) waiter;
            acceptNode.execute(executor, value);
        } else if (waiter instanceof ApplyNode) {
            ApplyNode applyNode = (ApplyNode) waiter;
            applyNode.execute(executor, value);
        } else if (waiter instanceof RunNode) {
            RunNode runNode = (RunNode) waiter;
            runNode.execute(executor, value);
        } else if (waiter instanceof AbstractInvocationFuture.WhenCompleteNode) {
            WhenCompleteNode whenCompleteNode = (WhenCompleteNode) waiter;
            Throwable t = (value instanceof ExceptionalResult) ? ((ExceptionalResult) value).cause : null;
            value = (value instanceof ExceptionalResult)? null : value;
            whenCompleteNode.execute(executor, value, t);
        } else if (waiter instanceof HandleNode) {
            HandleNode handleNode = (HandleNode) waiter;
            Throwable t = (value instanceof ExceptionalResult) ? ((ExceptionalResult) value).cause : null;
            value = (value instanceof ExceptionalResult)? null : value;
            handleNode.execute(executor, value, t);
        } else if (waiter instanceof ExceptionallyNode) {
            ((ExceptionallyNode) waiter).execute(value);
        } else if (waiter instanceof ComposeNode) {
            ((ComposeNode) waiter).execute(executor, value);
        } else if (waiter instanceof AbstractBiNode) {
            ((AbstractBiNode) waiter).execute(executor, value);
        } else if (waiter instanceof AbstractEitherNode) {
            ((AbstractEitherNode) waiter).execute(executor, value);
        }
    }

    <T> CompletableFuture<T> newCompletableFuture() {
        // todo
        return new CompletableFuture<T>();
    }

    // thenApply* implementation
    @Override
    public <U> CompletionStage<U> thenApply(@Nonnull Function<? super V, ? extends U> fn) {
        return unblock(fn, null);
    }

    @Override
    public <U> CompletionStage<U> thenApplyAsync(@Nonnull Function<? super V, ? extends U> fn) {
        return unblock(fn, defaultExecutor);
    }

    @Override
    public <U> CompletionStage<U> thenApplyAsync(@Nonnull Function<? super V, ? extends U> fn, Executor executor) {
        return unblock(fn, executor);
    }

    protected <U> CompletionStage<U> unblock(@Nonnull final Function<? super V, ? extends U> function, Executor executor) {
        requireNonNull(function);
        final Object value = resolve(state);
        final CompletableFuture<U> result = newCompletableFuture();
        if (value != UNRESOLVED) {
            if (cascadeException(value, result)) {
                return result;
            }
            Executor e = (executor == null) ? CALLER_RUNS : executor;
            e.execute(() -> {
                result.complete(function.apply((V) value));
            });
            return result;
        } else {
            registerWaiter(new ApplyNode(result, function), executor);
            return result;
        }
    }

    // thenRun implementation
    @Override
    public CompletionStage<Void> thenRun(@Nonnull Runnable action) {
        return unblock(action, null);
    }

    @Override
    public CompletionStage<Void> thenRunAsync(@Nonnull Runnable action) {
        return unblock(action, defaultExecutor);
    }

    @Override
    public CompletionStage<Void> thenRunAsync(@Nonnull Runnable action, Executor executor) {
        return unblock(action, executor);
    }

    protected CompletionStage<Void> unblock(@Nonnull final Runnable runnable, Executor executor) {
        requireNonNull(runnable);
        final Object value = resolve(state);
        final CompletableFuture<Void> result = newCompletableFuture();
        if (value != UNRESOLVED) {
            if (cascadeException(value, result)) {
                return result;
            }
            return runAfter0(result, runnable, executor);
        } else {
            registerWaiter(new RunNode(result, runnable), executor);
            return result;
        }
    }

    @Override
    public <U> CompletionStage<U> handle(@Nonnull BiFunction<? super V, Throwable, ? extends U> fn) {
        return unblock(fn, null);
    }

    @Override
    public <U> CompletionStage<U> handleAsync(@Nonnull BiFunction<? super V, Throwable, ? extends U> fn) {
        return unblock(fn, defaultExecutor);
    }

    @Override
    public <U> CompletionStage<U> handleAsync(@Nonnull BiFunction<? super V, Throwable, ? extends U> fn, Executor executor) {
        return unblock(fn, executor);
    }

    private <U> CompletionStage<U> unblock(@Nonnull BiFunction<? super V, Throwable, ? extends U> fn, Executor executor) {
        requireNonNull(fn);
        Object resolved = resolve(state);
        final CompletableFuture<U> future = newCompletableFuture();
        if (resolved != UNRESOLVED) {
            V value;
            Throwable throwable;
            if (resolved instanceof ExceptionalResult) {
                throwable = ((ExceptionalResult) resolved).cause;
                value = null;
            } else {
                throwable = null;
                value = (V) resolved;
            }

            if (executor != null) {
                executor.execute(() -> {
                    try {
                        U result = fn.apply(value, throwable);
                        future.complete(result);
                    } catch (Throwable t) {
                        future.completeExceptionally(t);
                    }
                });
            } else {
                try {
                    U result = fn.apply(value, throwable);
                    future.complete(result);
                } catch (Throwable t) {
                    future.completeExceptionally(t);
                }
            }
            return future;
        } else {
            registerWaiter(new HandleNode(future, fn), executor);
            return future;
        }
    }

    // whenComplete
    @Override
    public CompletionStage<V> whenComplete(@Nonnull BiConsumer<? super V, ? super Throwable> action) {
        return unblock(action, null);
    }

    @Override
    public CompletionStage<V> whenCompleteAsync(@Nonnull BiConsumer<? super V, ? super Throwable> action) {
        return unblock(action, defaultExecutor);
    }

    @Override
    public CompletionStage<V> whenCompleteAsync(@Nonnull BiConsumer<? super V, ? super Throwable> action, Executor executor) {
        return unblock(action, executor);
    }

    protected CompletionStage<V> unblock(@Nonnull final BiConsumer<? super V, ? super Throwable> biConsumer, Executor executor) {
        requireNonNull(biConsumer);
        Object result = resolve(state);
        final CompletableFuture<V> future = newCompletableFuture();
        if (result != UNRESOLVED && isDone()) {
            V value;
            Throwable throwable;
            if (result instanceof ExceptionalResult) {
                throwable = ((ExceptionalResult) result).cause;
                value = null;
            } else {
                throwable = null;
                value = (V) result;
            }
            Executor e = (executor == null) ? CALLER_RUNS : executor;
            e.execute(() -> {
                try {
                    biConsumer.accept((V) value, throwable);
                } catch (Throwable t) {
                    completeExceptionallyWithPriority(future, throwable, t);
                    return;
                }
                completeFuture(future, value, throwable);
            });
            return future;
        } else {
            registerWaiter(new WhenCompleteNode(future, biConsumer), executor);
            return future;
        }
    }

    @Override
    public CompletionStage<V> exceptionally(@Nonnull Function<Throwable, ? extends V> fn) {
        requireNonNull(fn);
        Object result = resolve(state);
        final CompletableFuture<V> future = newCompletableFuture();
        if (result != UNRESOLVED && isDone()) {
            if (result instanceof ExceptionalResult) {
                Throwable throwable = ((ExceptionalResult) result).cause;
                try {
                    V value = fn.apply(throwable);
                    future.complete(value);
                } catch (Throwable t) {
                    future.completeExceptionally(t);
                }
            } else {
                future.complete((V) result);
            }
            return future;
        } else {
            registerWaiter(new ExceptionallyNode<V>(future, fn), null);
            return future;
        }
    }

    @Override
    public <U> CompletionStage<U> thenCompose(@Nonnull Function<? super V, ? extends CompletionStage<U>> fn) {
        return unblockCompose(fn, null);
    }

    @Override
    public <U> CompletionStage<U> thenComposeAsync(@Nonnull Function<? super V, ? extends CompletionStage<U>> fn) {
        return unblockCompose(fn, defaultExecutor);
    }

    @Override
    public <U> CompletionStage<U> thenComposeAsync(@Nonnull Function<? super V, ? extends CompletionStage<U>> fn,
                                                   Executor executor) {
        return unblockCompose(fn, executor);
    }

    protected <U> CompletionStage<U> unblockCompose(@Nonnull final Function<? super V, ? extends CompletionStage<U>> function,
                                                    Executor executor) {
        requireNonNull(function);
        final Object value = resolve(state);

        CompletableFuture<U> result = newCompletableFuture();
        if (value != UNRESOLVED && isDone()) {
            if (cascadeException(value, result)) {
                return result;
            }
            Executor e = (executor == null) ? CALLER_RUNS : executor;
            e.execute(() -> {
                try {
                    CompletionStage<U> r = function.apply((V) value);
                    r.whenComplete((v, t) -> {
                        if (t == null) {
                            result.complete(v);
                        } else {
                            result.completeExceptionally(t);
                        }
                    });
                } catch (Throwable t) {
                    result.completeExceptionally(t);
                }
            });
            return result;
        } else {
            registerWaiter(new ComposeNode<>(result, function), executor);
            return result;
        }
    }

    ///// stubs to implement
    @Override
    public <U, R> CompletionStage<R> thenCombine(@Nonnull CompletionStage<? extends U> other,
                                                 @Nonnull BiFunction<? super V, ? super U, ? extends R> fn) {
        return unblockCombine(other, fn, null);
    }

    @Override
    public <U, R> CompletionStage<R> thenCombineAsync(CompletionStage<? extends U> other,
                                                        BiFunction<? super V, ? super U, ? extends R> fn) {
        return unblockCombine(other, fn, defaultExecutor);
    }

    @Override
    public <U, R> CompletionStage<R> thenCombineAsync(CompletionStage<? extends U> other,
                                                        BiFunction<? super V, ? super U, ? extends R> fn, Executor executor) {
        return unblockCombine(other, fn, executor);
    }

    protected <U, R> CompletionStage<R> unblockCombine(@Nonnull CompletionStage<? extends U> other,
                                                       @Nonnull final BiFunction<? super V, ? super U, ? extends R> function,
                                                       Executor executor) {
        requireNonNull(other);
        requireNonNull(function);
        final Object value = resolve(state);
        final CompletableFuture<? extends U> otherFuture =
                (other instanceof CompletableFuture) ? (CompletableFuture<? extends U>) other : other.toCompletableFuture();

        CompletableFuture<R> result = newCompletableFuture();
        if (value != UNRESOLVED && isDone()) {
            // TODO does this violate contract of exception handling in CompletionStage?
            //  thenCombine would wait for both futures to complete normally, but does not
            //  indicate that it is required to wait for both when completed exceptionally
            // in case this future is completed exceptionally, the result is also exceptionally completed
            // without checking whether otherFuture is completed or not
            if (cascadeException(value, result)) {
                return result;
            }
            if (!otherFuture.isDone()) {
                // register on other future as waiter and return
                otherFuture.whenCompleteAsync((v, t) -> {
                    if (t != null) {
                        result.completeExceptionally(t);
                    }
                    try {
                        R r = function.apply((V) value, v);
                        result.complete(r);
                    } catch (Throwable e) {
                        result.completeExceptionally(e);
                    }
                }, executor);
                return result;
            }
            // both futures are done
            if (otherFuture.isCompletedExceptionally()) {
                otherFuture.exceptionally(t -> {
                    result.completeExceptionally(t);
                    return null;
                });
                return result;
            }
            U otherValue = otherFuture.join();
            Executor e = (executor == null) ? CALLER_RUNS : executor;
            e.execute(() -> {
                try {
                    R r = function.apply((V) value, otherValue);
                    result.complete(r);
                } catch (Throwable t) {
                    result.completeExceptionally(t);
                }
            });
            return result;
        } else {
            CombineNode waiter = new CombineNode(result, otherFuture, function);
            registerWaiter(waiter, executor);
            return result;
        }
    }

    @Override
    public <U> CompletionStage<Void> thenAcceptBoth(CompletionStage<? extends U> other, BiConsumer<? super V, ? super U> action) {
        return unblockAcceptBoth(other, action, null);
    }

    @Override
    public <U> CompletionStage<Void> thenAcceptBothAsync(CompletionStage<? extends U> other,
                                                         BiConsumer<? super V, ? super U> action) {
        return unblockAcceptBoth(other, action, defaultExecutor);
    }

    @Override
    public <U> CompletionStage<Void> thenAcceptBothAsync(CompletionStage<? extends U> other,
                                                         BiConsumer<? super V, ? super U> action, Executor executor) {
        return unblockAcceptBoth(other, action, executor);
    }

    protected <U> CompletionStage<Void> unblockAcceptBoth(@Nonnull CompletionStage<? extends U> other,
                                                       @Nonnull final BiConsumer<? super V, ? super U> action,
                                                       Executor executor) {
        requireNonNull(other);
        requireNonNull(action);
        final Object value = resolve(state);
        final CompletableFuture<? extends U> otherFuture =
                (other instanceof CompletableFuture) ? (CompletableFuture<? extends U>) other : other.toCompletableFuture();

        CompletableFuture<Void> result = newCompletableFuture();
        if (value != UNRESOLVED && isDone()) {
            // TODO does this violate contract of exception handling in CompletionStage?
            //  thenCombine would wait for both futures to complete normally, but does not
            //  indicate that it is required to wait for both when completed exceptionally
            // in case this future is completed exceptionally, the result is also exceptionally completed
            // without checking whether otherFuture is completed or not
            if (cascadeException(value, result)) {
                return result;
            }
            if (!otherFuture.isDone()) {
                // register on other future as waiter and return
                otherFuture.whenCompleteAsync((u, t) -> {
                    if (t != null) {
                        result.completeExceptionally(t);
                    }
                    try {
                        action.accept((V) value, u);
                        result.complete(null);
                    } catch (Throwable e) {
                        result.completeExceptionally(e);
                    }
                }, executor);
                return result;
            }
            // both futures are done
            if (otherFuture.isCompletedExceptionally()) {
                otherFuture.exceptionally(t -> {
                    result.completeExceptionally(t);
                    return null;
                });
                return result;
            }
            U otherValue = otherFuture.join();
            Executor e = (executor == null) ? CALLER_RUNS : executor;
            e.execute(() -> {
                try {
                    action.accept((V) value, otherValue);
                    result.complete(null);
                } catch (Throwable t) {
                    result.completeExceptionally(t);
                }
            });
            return result;
        } else {
            AcceptBothNode waiter = new AcceptBothNode(result, otherFuture, action);
            registerWaiter(waiter, executor);
            return result;
        }
    }

    @Override
    public CompletionStage<Void> runAfterBoth(CompletionStage<?> other, Runnable action) {
        return unblockRunAfterBoth(other, action, null);
    }

    @Override
    public CompletionStage<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action) {
        return unblockRunAfterBoth(other, action, defaultExecutor);
    }

    @Override
    public CompletionStage<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action, Executor executor) {
        return unblockRunAfterBoth(other, action, executor);
    }

    protected <U> CompletionStage<Void> unblockRunAfterBoth(@Nonnull CompletionStage<? extends U> other,
                                                          @Nonnull final Runnable action,
                                                          Executor executor) {
        requireNonNull(other);
        requireNonNull(action);
        final Object value = resolve(state);
        final CompletableFuture<? extends U> otherFuture =
                (other instanceof CompletableFuture) ? (CompletableFuture<? extends U>) other : other.toCompletableFuture();

        CompletableFuture<Void> result = newCompletableFuture();
        if (value != UNRESOLVED && isDone()) {
            // TODO does this violate contract of exception handling in CompletionStage?
            //  thenCombine would wait for both futures to complete normally, but does not
            //  indicate that it is required to wait for both when completed exceptionally
            // in case this future is completed exceptionally, the result is also exceptionally completed
            // without checking whether otherFuture is completed or not
            if (cascadeException(value, result)) {
                return result;
            }
            if (!otherFuture.isDone()) {
                // register on other future as waiter and return
                otherFuture.whenCompleteAsync((u, t) -> {
                    if (t != null) {
                        result.completeExceptionally(t);
                    }
                    try {
                        action.run();
                        result.complete(null);
                    } catch (Throwable e) {
                        result.completeExceptionally(e);
                    }
                }, executor);
                return result;
            }
            // both futures are done
            if (otherFuture.isCompletedExceptionally()) {
                otherFuture.exceptionally(t -> {
                    result.completeExceptionally(t);
                    return null;
                });
                return result;
            }
            return runAfter0(result, action, executor);
        } else {
            RunAfterBothNode waiter = new RunAfterBothNode(result, otherFuture, action);
            registerWaiter(waiter, executor);
            return result;
        }
    }

    @Override
    public <U> CompletionStage<U> applyToEither(CompletionStage<? extends V> other, Function<? super V, U> fn) {
        return null;
    }

    @Override
    public <U> CompletionStage<U> applyToEitherAsync(CompletionStage<? extends V> other, Function<? super V, U> fn) {
        return null;
    }

    @Override
    public <U> CompletionStage<U> applyToEitherAsync(CompletionStage<? extends V> other, Function<? super V, U> fn,
                                                     Executor executor) {
        return null;
    }

    @Override
    public CompletionStage<Void> acceptEither(CompletionStage<? extends V> other, Consumer<? super V> action) {
        return null;
    }

    @Override
    public CompletionStage<Void> acceptEitherAsync(CompletionStage<? extends V> other, Consumer<? super V> action) {
        return null;
    }

    @Override
    public CompletionStage<Void> acceptEitherAsync(CompletionStage<? extends V> other, Consumer<? super V> action,
                                                   Executor executor) {
        return null;
    }

    @Override
    public CompletionStage<Void> runAfterEither(CompletionStage<?> other, Runnable action) {
        return unblockRunAfterEither(other, action, null);
    }

    @Override
    public CompletionStage<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action) {
        return unblockRunAfterEither(other, action, defaultExecutor);
    }

    @Override
    public CompletionStage<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action, Executor executor) {
        return unblockRunAfterEither(other, action, executor);
    }

    protected <U> CompletionStage<Void> unblockRunAfterEither(@Nonnull CompletionStage other,
                                                              @Nonnull final Runnable action,
                                                              Executor executor) {
        requireNonNull(other);
        requireNonNull(action);
        final Object value = resolve(state);
        final CompletableFuture<? extends V> otherFuture =
                (other instanceof CompletableFuture) ? (CompletableFuture<? extends V>) other : other.toCompletableFuture();

        CompletableFuture<Void> result = newCompletableFuture();
        if (value != UNRESOLVED && isDone()) {
            // in case this future is completed exceptionally, the result is also exceptionally completed
            if (cascadeException(value, result)) {
                return result;
            }
            return runAfter0(result, action, executor);
        } else if (otherFuture.isDone()) {
            if (otherFuture.isCompletedExceptionally()) {
                otherFuture.whenComplete((v, t) -> {
                    result.completeExceptionally(t);
                });
                return result;
            }
            return runAfter0(result, action, executor);
        } else {
            RunAfterEither waiter = new RunAfterEither(result, action);
            registerWaiter(waiter, executor);
            otherFuture.whenCompleteAsync(waiter, executor);
            return result;
        }
    }

    private CompletionStage<Void> runAfter0(CompletableFuture<Void> result, @Nonnull Runnable action, Executor executor) {
        Executor e = (executor == null) ? CALLER_RUNS : executor;
        e.execute(() -> {
            try {
                action.run();
                result.complete(null);
            } catch (Throwable t) {
                result.completeExceptionally(t);
            }
        });
        return result;
    }

    @Override
    public CompletableFuture<V> toCompletableFuture() {
        final InvocationCompletableFuture<V> completableFuture = new InvocationCompletableFuture<>(this);
        return completableFuture;
    }
}
