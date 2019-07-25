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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Wraps an existing InvocationCompletionStage, delegating CompletionStage & Future methods
 * while not allowing completion of the future
 */
public class InvocationCompletableFuture<T> extends CompletableFuture<T> {

    final InvocationCompletionStage<T> future;

    public InvocationCompletableFuture(InvocationCompletionStage<T> future) {
        this.future = future;
    }

    @Override
    public CompletableFuture<Void> thenAccept(Consumer<? super T> action) {
        return future.thenAccept(action).toCompletableFuture();
    }

    @Override
    public CompletableFuture<Void> thenAcceptAsync(Consumer<? super T> action) {
        return future.thenAcceptAsync(action).toCompletableFuture();
    }

    @Override
    public CompletableFuture<Void> thenAcceptAsync(Consumer<? super T> action, Executor executor) {
        return future.thenAcceptAsync(action, executor).toCompletableFuture();
    }

    @Override
    public <U> CompletableFuture<U> thenApply(Function<? super T, ? extends U> fn) {
        CompletionStage<U> completionStage = future.thenApply(fn);
        return completionStage.toCompletableFuture();
    }

    @Override
    public <U> CompletableFuture<U> thenApplyAsync(Function<? super T, ? extends U> fn) {
        CompletionStage<U> completionStage = future.thenApplyAsync(fn);
        return completionStage.toCompletableFuture();
    }

    @Override
    public <U> CompletableFuture<U> thenApplyAsync(Function<? super T, ? extends U> fn, Executor executor) {
        CompletionStage<U> completionStage = future.thenApplyAsync(fn, executor);
        return completionStage.toCompletableFuture();
    }

    @Override
    public CompletableFuture<Void> thenRun(Runnable action) {
        return future.thenRun(action).toCompletableFuture();
    }

    @Override
    public CompletableFuture<Void> thenRunAsync(Runnable action) {
        return future.thenRunAsync(action).toCompletableFuture();
    }

    @Override
    public CompletableFuture<Void> thenRunAsync(Runnable action, Executor executor) {
        return future.thenRunAsync(action, executor).toCompletableFuture();
    }

    @Override
    public <U> CompletableFuture<U> handle(BiFunction<? super T, Throwable, ? extends U> fn) {
        CompletionStage<U> completionStage = future.handle(fn);
        return completionStage.toCompletableFuture();
    }

    @Override
    public <U> CompletableFuture<U> handleAsync(BiFunction<? super T, Throwable, ? extends U> fn) {
        CompletionStage<U> completionStage = future.handleAsync(fn);
        return completionStage.toCompletableFuture();
    }

    @Override
    public <U> CompletableFuture<U> handleAsync(BiFunction<? super T, Throwable, ? extends U> fn, Executor executor) {
        CompletionStage<U> completionStage = future.handleAsync(fn, executor);
        return completionStage.toCompletableFuture();
    }

    @Override
    public CompletableFuture<T> whenComplete(BiConsumer<? super T, ? super Throwable> action) {
        return future.whenComplete(action).toCompletableFuture();
    }

    @Override
    public CompletableFuture<T> whenCompleteAsync(BiConsumer<? super T, ? super Throwable> action) {
        return future.whenCompleteAsync(action).toCompletableFuture();
    }

    @Override
    public CompletableFuture<T> whenCompleteAsync(BiConsumer<? super T, ? super Throwable> action, Executor executor) {
        return future.whenCompleteAsync(action, executor).toCompletableFuture();
    }

    @Override
    public <U> CompletableFuture<U> thenCompose(Function<? super T, ? extends CompletionStage<U>> fn) {
        return future.thenCompose(fn).toCompletableFuture();
    }

    @Override
    public <U> CompletableFuture<U> thenComposeAsync(Function<? super T, ? extends CompletionStage<U>> fn) {
        return future.thenComposeAsync(fn).toCompletableFuture();
    }

    @Override
    public <U> CompletableFuture<U> thenComposeAsync(Function<? super T, ? extends CompletionStage<U>> fn, Executor executor) {
        return future.thenComposeAsync(fn, executor).toCompletableFuture();
    }

    @Override
    public <U, V1> CompletableFuture<V1> thenCombine(CompletionStage<? extends U> other,
                                                   BiFunction<? super T, ? super U, ? extends V1> fn) {
        CompletionStage<V1> completionStage = future.thenCombine(other, fn);
        return completionStage.toCompletableFuture();
    }

    @Override
    public <U, V1> CompletableFuture<V1> thenCombineAsync(CompletionStage<? extends U> other,
                                                        BiFunction<? super T, ? super U, ? extends V1> fn) {
        CompletionStage<V1> completionStage = future.thenCombineAsync(other, fn);
        return completionStage.toCompletableFuture();
    }

    @Override
    public <U, V1> CompletableFuture<V1> thenCombineAsync(CompletionStage<? extends U> other,
                                                        BiFunction<? super T, ? super U, ? extends V1> fn, Executor executor) {
        CompletionStage<V1> completionStage = future.thenCombineAsync(other, fn, executor);
        return completionStage.toCompletableFuture();
    }

    @Override
    public <U> CompletableFuture<Void> thenAcceptBoth(CompletionStage<? extends U> other, BiConsumer<? super T, ? super U> action) {
        return future.thenAcceptBoth(other, action).toCompletableFuture();
    }

    @Override
    public <U> CompletableFuture<Void> thenAcceptBothAsync(CompletionStage<? extends U> other,
                                                         BiConsumer<? super T, ? super U> action) {
        return future.thenAcceptBothAsync(other, action).toCompletableFuture();
    }

    @Override
    public <U> CompletableFuture<Void> thenAcceptBothAsync(CompletionStage<? extends U> other,
                                                         BiConsumer<? super T, ? super U> action, Executor executor) {
        return future.thenAcceptBothAsync(other, action, executor).toCompletableFuture();
    }

    @Override
    public CompletableFuture<Void> runAfterBoth(CompletionStage<?> other, Runnable action) {
        return future.runAfterBoth(other, action).toCompletableFuture();
    }

    @Override
    public CompletableFuture<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action) {
        return future.runAfterBothAsync(other, action).toCompletableFuture();
    }

    @Override
    public CompletableFuture<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action, Executor executor) {
        return future.runAfterBothAsync(other, action, executor).toCompletableFuture();
    }

    @Override
    public <U> CompletableFuture<U> applyToEither(CompletionStage<? extends T> other, Function<? super T, U> fn) {
        return future.applyToEither(other, fn).toCompletableFuture();
    }

    @Override
    public <U> CompletableFuture<U> applyToEitherAsync(CompletionStage<? extends T> other, Function<? super T, U> fn) {
        return future.applyToEitherAsync(other, fn).toCompletableFuture();
    }

    @Override
    public <U> CompletableFuture<U> applyToEitherAsync(CompletionStage<? extends T> other, Function<? super T, U> fn,
                                                     Executor executor) {
        return future.applyToEitherAsync(other, fn, executor).toCompletableFuture();
    }

    @Override
    public CompletableFuture<Void> acceptEither(CompletionStage<? extends T> other, Consumer<? super T> action) {
        return future.acceptEither(other, action).toCompletableFuture();
    }

    @Override
    public CompletableFuture<Void> acceptEitherAsync(CompletionStage<? extends T> other, Consumer<? super T> action) {
        return future.acceptEitherAsync(other, action).toCompletableFuture();
    }

    @Override
    public CompletableFuture<Void> acceptEitherAsync(CompletionStage<? extends T> other, Consumer<? super T> action,
                                                   Executor executor) {
        return future.acceptEitherAsync(other, action, executor).toCompletableFuture();
    }

    @Override
    public CompletableFuture<Void> runAfterEither(CompletionStage<?> other, Runnable action) {
        return future.runAfterEither(other, action).toCompletableFuture();
    }

    @Override
    public CompletableFuture<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action) {
        return future.runAfterEitherAsync(other, action).toCompletableFuture();
    }

    @Override
    public CompletableFuture<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action, Executor executor) {
        return future.runAfterEitherAsync(other, action, executor).toCompletableFuture();
    }

    @Override
    public CompletableFuture<T> exceptionally(Function<Throwable, ? extends T> fn) {
        return future.exceptionally(fn).toCompletableFuture();
    }

    @Override
    public CompletableFuture<T> toCompletableFuture() {
        return this;
    }

    // CompletableFuture API methods
    @Override
    public boolean isDone() {
        return future.isDone();
    }

    @Override
    public T get()
            throws InterruptedException, ExecutionException {
        return future.get();
    }

    @Override
    public T get(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        return future.get(timeout, unit);
    }

    @Override
    public T join() {
        return future.join();
    }

    @Override
    public T getNow(T valueIfAbsent) {
        // todo
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public boolean complete(T value) {
        throw new UnsupportedOperationException("Not allowed");
    }

    @Override
    public boolean completeExceptionally(Throwable ex) {
        throw new UnsupportedOperationException("Not allowed");
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        throw new UnsupportedOperationException("Not allowed");
    }

    @Override
    public boolean isCancelled() {
        return future.isCancelled();
    }

    @Override
    public boolean isCompletedExceptionally() {
        return future.isCompletedExceptionally();
    }

    @Override
    public void obtrudeValue(T value) {
        throw new UnsupportedOperationException("Not allowed");
    }

    @Override
    public void obtrudeException(Throwable ex) {
        throw new UnsupportedOperationException("Not allowed");
    }

    @Override
    public int getNumberOfDependents() {
        // todo
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public String toString() {
        return future.toString();
    }
}
