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

package com.hazelcast.concurrent.atomiclong;

import com.hazelcast.concurrent.atomiclong.operations.AddAndGetOperation;
import com.hazelcast.concurrent.atomiclong.operations.AlterAndGetOperation;
import com.hazelcast.concurrent.atomiclong.operations.AlterOperation;
import com.hazelcast.concurrent.atomiclong.operations.ApplyOperation;
import com.hazelcast.concurrent.atomiclong.operations.CompareAndSetOperation;
import com.hazelcast.concurrent.atomiclong.operations.GetAndAddOperation;
import com.hazelcast.concurrent.atomiclong.operations.GetAndAlterOperation;
import com.hazelcast.concurrent.atomiclong.operations.GetAndSetOperation;
import com.hazelcast.concurrent.atomiclong.operations.GetOperation;
import com.hazelcast.concurrent.atomiclong.operations.SetOperation;
import com.hazelcast.core.AsyncAtomicLong;
import com.hazelcast.core.IFunction;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;

import java.util.concurrent.CompletableFuture;

import static com.hazelcast.util.Preconditions.isNotNull;

public class AtomicLongProxy extends AbstractDistributedObject<AtomicLongService> implements AsyncAtomicLong {

    private final String name;
    private final int partitionId;

    public AtomicLongProxy(String name, NodeEngine nodeEngine, AtomicLongService service) {
        super(nodeEngine, service);
        this.name = name;
        this.partitionId = nodeEngine.getPartitionService().getPartitionId(getNameAsPartitionAwareData());
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
        return AtomicLongService.SERVICE_NAME;
    }

    @Override
    public long addAndGet(long delta) {
        return addAndGetAsync(delta).join();
    }

    @Override
    public CompletableFuture<Long> addAndGetAsync(long delta) {
        Operation operation = new AddAndGetOperation(name, delta).setPartitionId(partitionId);
        return invokeOnPartition(operation);
    }

    @Override
    public CompletableFuture<Long> asyncAddAndGet(long delta) {
        return addAndGetAsync(delta);
    }

    @Override
    public boolean compareAndSet(long expect, long update) {
        return compareAndSetAsync(expect, update).join();
    }

    @Override
    public CompletableFuture<Boolean> compareAndSetAsync(long expect, long update) {
        Operation operation = new CompareAndSetOperation(name, expect, update)
                .setPartitionId(partitionId);
        return invokeOnPartition(operation);
    }

    @Override
    public CompletableFuture<Boolean> asyncCompareAndSet(long expect, long update) {
        return compareAndSetAsync(expect, update);
    }

    @Override
    public void set(long newValue) {
        setAsync(newValue).join();
    }

    @Override
    public CompletableFuture<Void> setAsync(long newValue) {
        Operation operation = new SetOperation(name, newValue)
                .setPartitionId(partitionId);
        return invokeOnPartition(operation);
    }

    @Override
    public CompletableFuture<Void> asyncSet(long newValue) {
        return setAsync(newValue);
    }

    @Override
    public long getAndSet(long newValue) {
        return getAndSetAsync(newValue).join();
    }

    @Override
    public CompletableFuture<Long> getAndSetAsync(long newValue) {
        Operation operation = new GetAndSetOperation(name, newValue)
                .setPartitionId(partitionId);
        return invokeOnPartition(operation);
    }

    @Override
    public CompletableFuture<Long> asyncGetAndSet(long newValue) {
        return getAndSetAsync(newValue);
    }

    @Override
    public long getAndAdd(long delta) {
        return getAndAddAsync(delta).join();
    }

    @Override
    public CompletableFuture<Long> getAndAddAsync(long delta) {
        Operation operation = new GetAndAddOperation(name, delta)
                .setPartitionId(partitionId);
        return invokeOnPartition(operation);
    }

    @Override
    public CompletableFuture<Long> asyncGetAndAdd(long delta) {
        return getAndAddAsync(delta);
    }

    @Override
    public long decrementAndGet() {
        return decrementAndGetAsync().join();
    }

    @Override
    public CompletableFuture<Long> decrementAndGetAsync() {
        return addAndGetAsync(-1);
    }

    @Override
    public CompletableFuture<Long> asyncDecrementAndGet() {
        return addAndGetAsync(-1);
    }

    @Override
    public long get() {
        return getAsync().join();
    }

    @Override
    public CompletableFuture<Long> getAsync() {
        Operation operation = new GetOperation(name)
                .setPartitionId(partitionId);
        return invokeOnPartition(operation);
    }

    @Override
    public CompletableFuture<Long> asyncGet() {
        return getAsync();
    }

    @Override
    public long incrementAndGet() {
        return incrementAndGetAsync().join();
    }

    @Override
    public CompletableFuture<Long> incrementAndGetAsync() {
        return addAndGetAsync(1);
    }

    @Override
    public CompletableFuture<Long> asyncIncrementAndGet() {
        return addAndGetAsync(1);
    }

    @Override
    public long getAndIncrement() {
        return getAndIncrementAsync().join();
    }

    @Override
    public CompletableFuture<Long> getAndIncrementAsync() {
        return getAndAddAsync(1);
    }

    @Override
    public CompletableFuture<Long> asyncGetAndIncrement() {
        return getAndAddAsync(1);
    }

    @Override
    public void alter(IFunction<Long, Long> function) {
        alterAsync(function).join();
    }

    @Override
    public CompletableFuture<Void> alterAsync(IFunction<Long, Long> function) {
        isNotNull(function, "function");

        Operation operation = new AlterOperation(name, function)
                .setPartitionId(partitionId);
        return invokeOnPartition(operation);
    }

    @Override
    public CompletableFuture<Void> asyncAlter(IFunction<Long, Long> function) {
        return alterAsync(function);
    }

    @Override
    public long alterAndGet(IFunction<Long, Long> function) {
        return alterAndGetAsync(function).join();
    }

    @Override
    public CompletableFuture<Long> alterAndGetAsync(IFunction<Long, Long> function) {
        isNotNull(function, "function");

        Operation operation = new AlterAndGetOperation(name, function)
                .setPartitionId(partitionId);
        return invokeOnPartition(operation);
    }

    @Override
    public CompletableFuture<Long> asyncAlterAndGet(IFunction<Long, Long> function) {
        return alterAndGetAsync(function);
    }

    @Override
    public long getAndAlter(IFunction<Long, Long> function) {
        return getAndAlterAsync(function).join();
    }

    @Override
    public CompletableFuture<Long> getAndAlterAsync(IFunction<Long, Long> function) {
        isNotNull(function, "function");

        Operation operation = new GetAndAlterOperation(name, function)
                .setPartitionId(partitionId);
        return invokeOnPartition(operation);
    }

    @Override
    public CompletableFuture<Long> asyncGetAndAlter(IFunction<Long, Long> function) {
        return getAndAlterAsync(function);
    }

    @Override
    public <R> R apply(IFunction<Long, R> function) {
        return applyAsync(function).join();
    }

    @Override
    public <R> CompletableFuture<R> applyAsync(IFunction<Long, R> function) {
        isNotNull(function, "function");

        Operation operation = new ApplyOperation<R>(name, function)
                .setPartitionId(partitionId);
        return invokeOnPartition(operation);
    }

    @Override
    public <R> CompletableFuture<R> asyncApply(IFunction<Long, R> function) {
        return applyAsync(function);
    }

    @Override
    public String toString() {
        return "IAtomicLong{" + "name='" + name + '\'' + '}';
    }
}
