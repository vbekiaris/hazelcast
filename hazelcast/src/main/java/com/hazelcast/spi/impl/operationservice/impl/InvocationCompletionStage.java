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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

// todo support exceptional result: null and throwables should be a separate outcome boxed in an ExceptionalResult
//  rather than a value of type Throwable
// todo check type arguments
// todo deduplication of executor==null/executor.execute branches
// todo deduplication of Nodes code (possible to extract interface?)
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
        if (waiter instanceof AcceptNode) {
            AcceptNode acceptNode = (AcceptNode) waiter;
            Object value = resolve(state);
            acceptNode.execute(executor, value);
        } else if (waiter instanceof ApplyNode) {
            ApplyNode applyNode = (ApplyNode) waiter;
            Object value = resolve(state);
            applyNode.execute(executor, value);
        } else if (waiter instanceof RunNode) {
            RunNode runNode = (RunNode) waiter;
            runNode.execute(executor);
        }
    }

    <T> CompletableFuture<T> newCompletableFuture() {
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

    // todo thenCompose, handle and whenComplete implementations
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

    @Override
    public <U> CompletionStage<U> handle(BiFunction<? super V, Throwable, ? extends U> fn) {
        return null;
    }

    @Override
    public <U> CompletionStage<U> handleAsync(BiFunction<? super V, Throwable, ? extends U> fn) {
        return null;
    }

    @Override
    public <U> CompletionStage<U> handleAsync(BiFunction<? super V, Throwable, ? extends U> fn, Executor executor) {
        return null;
    }

    @Override
    public CompletionStage<V> whenComplete(BiConsumer<? super V, ? super Throwable> action) {
        return null;
    }

    @Override
    public CompletionStage<V> whenCompleteAsync(BiConsumer<? super V, ? super Throwable> action) {
        return null;
    }

    @Override
    public CompletionStage<V> whenCompleteAsync(BiConsumer<? super V, ? super Throwable> action, Executor executor) {
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
        return null;
    }
}
