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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

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
// todo deduplication of executor==null/executor.execute branches
// todo deduplication of WaitNodes code (possible to extract interface?)
// todo pull CompletionStage implementation to AbstractInvocationFuture? or compose to other class for reuse in client-side futures?
public class InvocationCompletionStage<V> extends InvocationFuture<V> implements CompletionStage<V> {

    public InvocationCompletionStage(Invocation invocation, boolean deserialize) {
        super(invocation, deserialize);
    }

    // thenAccept* implementation
    @Override
    public CompletionStage<Void> thenAccept(Consumer<? super V> action) {
        return unblock(action, null);
    }

    @Override
    public CompletionStage<Void> thenAcceptAsync(Consumer<? super V> action) {
        return unblock(action, defaultExecutor);
    }

    @Override
    public CompletionStage<Void> thenAcceptAsync(Consumer<? super V> action, Executor executor) {
        return unblock(action, executor);
    }

    protected CompletionStage<Void> unblock(final Consumer<? super V> consumer, Executor executor) {
        final Object value = resolve(state);
        final CompletableFuture<Void> result = newCompletableFuture();
        if (value != UNRESOLVED) {
            if (cascadeException(value, result)) {
                return result;
            }
            if (executor != null) {
                executor.execute(() -> {
                    consumer.accept((V) value);
                    result.complete(null);
                });
            } else {
                consumer.accept((V) value);
                result.complete(null);
            }
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
        }
    }

    <T> CompletableFuture<T> newCompletableFuture() {
        // todo
        return new CompletableFuture<T>();
    }

    // thenApply* implementation
    @Override
    public <U> CompletionStage<U> thenApply(Function<? super V, ? extends U> fn) {
        return unblock(fn, null);
    }

    @Override
    public <U> CompletionStage<U> thenApplyAsync(Function<? super V, ? extends U> fn) {
        return unblock(fn, defaultExecutor);
    }

    @Override
    public <U> CompletionStage<U> thenApplyAsync(Function<? super V, ? extends U> fn, Executor executor) {
        return unblock(fn, executor);
    }

    protected <U> CompletionStage<U> unblock(final Function<? super V, ? extends U> function, Executor executor) {
        final Object value = resolve(state);
        final CompletableFuture<U> result = newCompletableFuture();
        if (value != UNRESOLVED) {
            if (cascadeException(value, result)) {
                return result;
            }
            if (executor != null) {
                executor.execute(() -> {
                    result.complete(function.apply((V) value));
                });
            } else {
                result.complete(function.apply((V) value));
            }
            return result;
        } else {
            registerWaiter(new ApplyNode(result, function), executor);
            return result;
        }
    }

    // thenRun implementation
    @Override
    public CompletionStage<Void> thenRun(Runnable action) {
        return unblock(action, null);
    }

    @Override
    public CompletionStage<Void> thenRunAsync(Runnable action) {
        return unblock(action, defaultExecutor);
    }

    @Override
    public CompletionStage<Void> thenRunAsync(Runnable action, Executor executor) {
        return unblock(action, executor);
    }

    protected CompletionStage<Void> unblock(final Runnable runnable, Executor executor) {
        final Object value = resolve(state);
        final CompletableFuture<Void> result = newCompletableFuture();
        if (value != UNRESOLVED) {
            if (cascadeException(value, result)) {
                return result;
            }
            if (executor != null) {
                executor.execute(() -> {
                    runnable.run();
                    result.complete(null);
                });
            } else {
                runnable.run();
                result.complete(null);
            }
            return result;
        } else {
            registerWaiter(new RunNode(result, runnable), executor);
            return result;
        }
    }

    @Override
    public <U> CompletionStage<U> handle(BiFunction<? super V, Throwable, ? extends U> fn) {
        return unblock(fn, null);
    }

    @Override
    public <U> CompletionStage<U> handleAsync(BiFunction<? super V, Throwable, ? extends U> fn) {
        return unblock(fn, defaultExecutor);
    }

    @Override
    public <U> CompletionStage<U> handleAsync(BiFunction<? super V, Throwable, ? extends U> fn, Executor executor) {
        return unblock(fn, executor);
    }

    private <U> CompletionStage<U> unblock(BiFunction<? super V, Throwable, ? extends U> fn, Executor executor) {
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
    public CompletionStage<V> whenComplete(BiConsumer<? super V, ? super Throwable> action) {
        return unblock(action, null);
    }

    @Override
    public CompletionStage<V> whenCompleteAsync(BiConsumer<? super V, ? super Throwable> action) {
        return unblock(action, defaultExecutor);
    }

    @Override
    public CompletionStage<V> whenCompleteAsync(BiConsumer<? super V, ? super Throwable> action, Executor executor) {
        return unblock(action, executor);
    }

    protected CompletionStage<V> unblock(final BiConsumer<? super V, ? super Throwable> runnable, Executor executor) {
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
            if (executor != null) {
                executor.execute(() -> {
                    try {
                        runnable.accept((V) value, throwable);
                    } catch (Throwable t) {
                        completeExceptionallyWithPriority(future, throwable, t);
                        return;
                    }
                    completeFuture(future, value, throwable);
                });
            } else {
                try {
                    runnable.accept((V) value, throwable);
                } catch (Throwable t) {
                    completeExceptionallyWithPriority(future, throwable, t);
                    return future;
                }
                completeFuture(future, value, throwable);
            }
            return future;
        } else {
            registerWaiter(new WhenCompleteNode(future, runnable), executor);
            return future;
        }
    }

    // todo thenCompose, thenCombine implementations
    // todo another kind of node per method family

    @Override
    public <U> CompletionStage<U> thenCompose(Function<? super V, ? extends CompletionStage<U>> fn) {
        return null;
    }

    @Override
    public <U> CompletionStage<U> thenComposeAsync(Function<? super V, ? extends CompletionStage<U>> fn) {
        return null;
    }

    @Override
    public <U> CompletionStage<U> thenComposeAsync(Function<? super V, ? extends CompletionStage<U>> fn, Executor executor) {
        return null;
    }

    ///// stubs to implement
    @Override
    public <U, V1> CompletionStage<V1> thenCombine(CompletionStage<? extends U> other,
                                                   BiFunction<? super V, ? super U, ? extends V1> fn) {
        return null;
    }

    @Override
    public <U, V1> CompletionStage<V1> thenCombineAsync(CompletionStage<? extends U> other,
                                                        BiFunction<? super V, ? super U, ? extends V1> fn) {
        return null;
    }

    @Override
    public <U, V1> CompletionStage<V1> thenCombineAsync(CompletionStage<? extends U> other,
                                                        BiFunction<? super V, ? super U, ? extends V1> fn, Executor executor) {
        return null;
    }

    @Override
    public <U> CompletionStage<Void> thenAcceptBoth(CompletionStage<? extends U> other, BiConsumer<? super V, ? super U> action) {
        return null;
    }

    @Override
    public <U> CompletionStage<Void> thenAcceptBothAsync(CompletionStage<? extends U> other,
                                                         BiConsumer<? super V, ? super U> action) {
        return null;
    }

    @Override
    public <U> CompletionStage<Void> thenAcceptBothAsync(CompletionStage<? extends U> other,
                                                         BiConsumer<? super V, ? super U> action, Executor executor) {
        return null;
    }

    @Override
    public CompletionStage<Void> runAfterBoth(CompletionStage<?> other, Runnable action) {
        return null;
    }

    @Override
    public CompletionStage<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action) {
        return null;
    }

    @Override
    public CompletionStage<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action, Executor executor) {
        return null;
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
        return null;
    }

    @Override
    public CompletionStage<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action) {
        return null;
    }

    @Override
    public CompletionStage<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action, Executor executor) {
        return null;
    }

    @Override
    public CompletionStage<V> exceptionally(Function<Throwable, ? extends V> fn) {
        return null;
    }

    @Override
    public CompletableFuture<V> toCompletableFuture() {
        final InvocationCompletableFuture<V> completableFuture = new InvocationCompletableFuture<>(this);
        return completableFuture;
    }
}
