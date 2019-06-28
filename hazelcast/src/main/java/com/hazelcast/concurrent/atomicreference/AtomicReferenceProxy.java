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

package com.hazelcast.concurrent.atomicreference;

import com.hazelcast.concurrent.atomicreference.operations.AlterAndGetOperation;
import com.hazelcast.concurrent.atomicreference.operations.AlterOperation;
import com.hazelcast.concurrent.atomicreference.operations.ApplyOperation;
import com.hazelcast.concurrent.atomicreference.operations.CompareAndSetOperation;
import com.hazelcast.concurrent.atomicreference.operations.ContainsOperation;
import com.hazelcast.concurrent.atomicreference.operations.GetAndAlterOperation;
import com.hazelcast.concurrent.atomicreference.operations.GetAndSetOperation;
import com.hazelcast.concurrent.atomicreference.operations.GetOperation;
import com.hazelcast.concurrent.atomicreference.operations.IsNullOperation;
import com.hazelcast.concurrent.atomicreference.operations.SetAndGetOperation;
import com.hazelcast.concurrent.atomicreference.operations.SetOperation;
import com.hazelcast.core.AsyncAtomicReference;
import com.hazelcast.core.IFunction;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;

import java.util.concurrent.CompletableFuture;

import static com.hazelcast.util.Preconditions.isNotNull;

public class AtomicReferenceProxy<E> extends AbstractDistributedObject<AtomicReferenceService>
        implements AsyncAtomicReference<E> {

    private final String name;
    private final int partitionId;

    public AtomicReferenceProxy(String name, NodeEngine nodeEngine, AtomicReferenceService service) {
        super(nodeEngine, service);
        this.name = name;
        this.partitionId = nodeEngine.getPartitionService().getPartitionId(getNameAsPartitionAwareData());
    }

    @Override
    public void alter(IFunction<E, E> function) {
        alterAsync(function).join();
    }

    @Override
    public CompletableFuture<Void> alterAsync(IFunction<E, E> function) {
        isNotNull(function, "function");

        Operation operation = new AlterOperation(name, toData(function))
                .setPartitionId(partitionId);
        return invokeOnPartition(operation);
    }

    @Override
    public CompletableFuture<Void> asyncAlter(IFunction<E, E> function) {
        return alterAsync(function);
    }

    @Override
    public E alterAndGet(IFunction<E, E> function) {
        return alterAndGetAsync(function).join();
    }

    @Override
    public CompletableFuture<E> alterAndGetAsync(IFunction<E, E> function) {
        isNotNull(function, "function");

        Operation operation = new AlterAndGetOperation(name, toData(function))
                .setPartitionId(partitionId);
        return invokeOnPartition(operation);
    }

    @Override
    public CompletableFuture<E> asyncAlterAndGet(IFunction<E, E> function) {
        return alterAndGetAsync(function);
    }

    @Override
    public E getAndAlter(IFunction<E, E> function) {
        return getAndAlterAsync(function).join();
    }

    @Override
    public CompletableFuture<E> getAndAlterAsync(IFunction<E, E> function) {
        isNotNull(function, "function");

        Operation operation = new GetAndAlterOperation(name, toData(function))
                .setPartitionId(partitionId);
        return invokeOnPartition(operation);
    }

    @Override
    public CompletableFuture<E> asyncGetAndAlter(IFunction<E, E> function) {
        return getAndAlterAsync(function);
    }

    @Override
    public <R> R apply(IFunction<E, R> function) {
        return applyAsync(function).join();
    }

    @Override
    public <R> CompletableFuture<R> applyAsync(IFunction<E, R> function) {
        isNotNull(function, "function");

        Operation operation = new ApplyOperation(name, toData(function))
                .setPartitionId(partitionId);
        return invokeOnPartition(operation);
    }

    @Override
    public <R> CompletableFuture<R> asyncApply(IFunction<E, R> function) {
        return applyAsync(function);
    }

    @Override
    public void clear() {
        clearAsync().join();
    }

    @Override
    public CompletableFuture<Void> clearAsync() {
        return setAsync(null);
    }

    @Override
    public CompletableFuture<Void> asyncClear() {
        return clearAsync();
    }

    @Override
    public boolean compareAndSet(E expect, E update) {
        return compareAndSetAsync(expect, update).join();
    }

    @Override
    public CompletableFuture<Boolean> compareAndSetAsync(E expect, E update) {
        Operation operation = new CompareAndSetOperation(name, toData(expect), toData(update))
                .setPartitionId(partitionId);
        return invokeOnPartition(operation);
    }

    @Override
    public CompletableFuture<Boolean> asyncCompareAndSet(E expect, E update) {
        return compareAndSetAsync(expect, update);
    }

    @Override
    public E get() {
        return getAsync().join();
    }

    @Override
    public CompletableFuture<E> getAsync() {
        Operation operation = new GetOperation(name)
                .setPartitionId(partitionId);
        return invokeOnPartition(operation);
    }

    @Override
    public CompletableFuture<E> asyncGet() {
        return getAsync();
    }

    @Override
    public boolean contains(E expected) {
        return containsAsync(expected).join();
    }

    @Override
    public CompletableFuture<Boolean> containsAsync(E expected) {
        Operation operation = new ContainsOperation(name, toData(expected))
                .setPartitionId(partitionId);
        return invokeOnPartition(operation);
    }

    @Override
    public CompletableFuture<Boolean> asyncContains(E value) {
        return containsAsync(value);
    }

    @Override
    public void set(E newValue) {
        setAsync(newValue).join();
    }

    @Override
    public CompletableFuture<Void> setAsync(E newValue) {
        Operation operation = new SetOperation(name, toData(newValue))
                .setPartitionId(partitionId);
        return invokeOnPartition(operation);
    }

    @Override
    public CompletableFuture<Void> asyncSet(E newValue) {
        Operation operation = new SetOperation(name, toData(newValue))
                .setPartitionId(partitionId);
        return invokeOnPartition(operation);
    }

    @Override
    public E getAndSet(E newValue) {
        return getAndSetAsync(newValue).join();
    }

    @Override
    public CompletableFuture<E> getAndSetAsync(E newValue) {
        Operation operation = new GetAndSetOperation(name, toData(newValue))
                .setPartitionId(partitionId);
        return invokeOnPartition(operation);
    }

    @Override
    public CompletableFuture<E> asyncGetAndSet(E newValue) {
        return getAndSetAsync(newValue);
    }

    @Override
    public E setAndGet(E update) {
        return asyncSetAndGet(update).join();
    }

    @Override
    public CompletableFuture<E> asyncSetAndGet(E update) {
        Operation operation = new SetAndGetOperation(name, toData(update))
                .setPartitionId(partitionId);
        return invokeOnPartition(operation);
    }

    @Override
    public boolean isNull() {
        return isNullAsync().join();
    }

    @Override
    public CompletableFuture<Boolean> isNullAsync() {
        Operation operation = new IsNullOperation(name)
                .setPartitionId(partitionId);
        return invokeOnPartition(operation);
    }

    @Override
    public CompletableFuture<Boolean> asyncIsNull() {
        return isNullAsync();
    }

    @Override
    public String getName() {
        return name;
    }

    public int getPartitionId() {
        return partitionId;
    }

    @Override
    public String getServiceName() {
        return AtomicReferenceService.SERVICE_NAME;
    }

    @Override
    public String toString() {
        return "IAtomicReference{" + "name='" + name + '\'' + '}';
    }
}
