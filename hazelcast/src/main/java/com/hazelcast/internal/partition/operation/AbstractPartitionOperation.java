/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.partition.operation;

import com.hazelcast.core.Offloadable;
import com.hazelcast.internal.partition.FragmentedMigrationAwareService;
import com.hazelcast.internal.partition.MigrationAwareService;
import com.hazelcast.internal.partition.NonFragmentedServiceNamespace;
import com.hazelcast.internal.partition.OffloadedReplicationPreparation;
import com.hazelcast.internal.partition.PartitionReplicationEvent;
import com.hazelcast.internal.partition.impl.PartitionDataSerializerHook;
import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.impl.operationexecutor.impl.PartitionOperationThread;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.servicemanager.ServiceInfo;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;

abstract class AbstractPartitionOperation extends Operation implements IdentifiedDataSerializable {

    final Collection<MigrationAwareService> getMigrationAwareServices() {
        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        return nodeEngine.getServices(MigrationAwareService.class);
    }

    final Collection<Operation> createAllReplicationOperations(PartitionReplicationEvent event) {
        return createReplicationOperations(event, false);
    }

    final Collection<Operation> createNonFragmentedReplicationOperations(PartitionReplicationEvent event) {
        return createReplicationOperations(event, true);
    }

    private Collection<Operation> createReplicationOperations(PartitionReplicationEvent event, boolean nonFragmentedOnly) {
        Collection<Operation> operations = new ArrayList<>();
        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        Collection<ServiceInfo> services = nodeEngine.getServiceInfos(MigrationAwareService.class);

        for (ServiceInfo serviceInfo : services) {
            MigrationAwareService service = serviceInfo.getService();
            if (nonFragmentedOnly && service instanceof FragmentedMigrationAwareService) {
                // skip fragmented services
                continue;
            }

            Operation op = service.prepareReplicationOperation(event);
            if (op != null) {
                op.setServiceName(serviceInfo.getName());
                operations.add(op);
            }
        }
        return operations;
    }

    // for invocation within partition thread; prepares replication operations within partition thread
    // does not support differential sync
    final Collection<Operation> createFragmentReplicationOperations(PartitionReplicationEvent event, ServiceNamespace ns) {
        assert !(ns instanceof NonFragmentedServiceNamespace) : ns + " should be used only for non-fragmented services!";

        Collection<Operation> operations = emptySet();
        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        Collection<ServiceInfo> services = nodeEngine.getServiceInfos(FragmentedMigrationAwareService.class);

        for (ServiceInfo serviceInfo : services) {
            FragmentedMigrationAwareService service = serviceInfo.getService();
            if (!service.isKnownServiceNamespace(ns)) {
                continue;
            }

            operations = prepareAndAppendReplicationOperation(event, ns, service, serviceInfo.getName(), operations);
        }
        return operations;
    }

    final Collection<Operation> createFragmentReplicationOperationsOffload(PartitionReplicationEvent event, ServiceNamespace ns,
                                                                           Collection<String> serviceNames) {
        assert !(ns instanceof NonFragmentedServiceNamespace) : ns + " should be used only for non-fragmented services!";

        final boolean runsOnPartitionThread = Thread.currentThread() instanceof PartitionOperationThread;
        Collection<Operation> operations = emptySet();
        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        for (String serviceName : serviceNames) {
            FragmentedMigrationAwareService service = nodeEngine.getService(serviceName);
            assert service.isKnownServiceNamespace(ns) : ns + " should be known by " + service;

            operations = collectReplicationOperations(event, ns, runsOnPartitionThread, operations, serviceName, service);
        }

        return operations;
    }

    // todo RU_COMPAT_4_2 -- to be used by PartitionReplicaSyncRequest for offloaded replication op preparation
    final Collection<Operation> createFragmentReplicationOperationsOffload(PartitionReplicationEvent event, ServiceNamespace ns) {
        assert !(ns instanceof NonFragmentedServiceNamespace) : ns + " should be used only for non-fragmented services!";

        final boolean runsOnPartitionThread = Thread.currentThread() instanceof PartitionOperationThread;
        Collection<Operation> operations = emptySet();
        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        Collection<ServiceInfo> services = nodeEngine.getServiceInfos(FragmentedMigrationAwareService.class);

        for (ServiceInfo serviceInfo : services) {
            FragmentedMigrationAwareService service = serviceInfo.getService();
            if (!service.isKnownServiceNamespace(ns)) {
                continue;
            }
            operations = collectReplicationOperations(event, ns, runsOnPartitionThread, operations,
                    serviceInfo.getName(), service);
        }
        return operations;
    }

    /**
     * Collect replication operations of a single fragmented service.
     * If the service implements {@link Offloadable} interface, then
     * that service's {@link FragmentedMigrationAwareService#prepareReplicationOperation(PartitionReplicationEvent, Collection)}
     * method will be invoked from the internal {@code ASYNC_EXECUTOR},
     * otherwise it will be invoked from the partition thread.
     */
    @Nullable
    private Collection<Operation> collectReplicationOperations(PartitionReplicationEvent event, ServiceNamespace ns,
                                         boolean runsOnPartitionThread, Collection<Operation> operations,
                                         String serviceName, FragmentedMigrationAwareService service) {
        // execute on current thread iff (OffloadedReplicationPreparation && !runsOnPartitionThread)
        // or (!OffloadedReplicationPreparation && runsOnPartitionThread), otherwise explicitly
        // request execution on partition thread.
        if (service instanceof OffloadedReplicationPreparation
                && ((OffloadedReplicationPreparation) service).shouldOffload()) {
            if (!runsOnPartitionThread) {
                // execute outside partition thread
                assert (!(Thread.currentThread() instanceof PartitionOperationThread)) : "Offloadable replication op"
                        + " preparation must be executed on non-partition thread";
                operations = prepareAndAppendReplicationOperation(event, ns, service,
                        serviceName, operations);
            } else {
                // execute on async executor
                Collection<Operation> mutableOps = new ArrayList<>();
                Future<?> future =
                        getNodeEngine().getExecutionService().submit(ExecutionService.ASYNC_EXECUTOR,
                        () -> prepareAndAppendReplicationOperationMutable(event, ns, service, serviceName, mutableOps));
                try {
                    future.get();
                    operations = postProcessOperations(operations, mutableOps);
                } catch (ExecutionException | CancellationException e) {
                    ExceptionUtil.rethrow(e.getCause());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    ExceptionUtil.rethrow(e);
                }
            }
        } else {
            // always execute in partition thread
            if (runsOnPartitionThread) {
                assert (Thread.currentThread() instanceof PartitionOperationThread) : "Replication op"
                        + " preparation must be executed on partition thread";
                operations = prepareAndAppendReplicationOperation(event, ns, service, serviceName, operations);
            } else {
                Collection<Operation> mutableOps = new ArrayList<>();
                RunOnPartitionThread partitionThreadRunnable = new RunOnPartitionThread(
                        () -> prepareAndAppendReplicationOperationMutable(event, ns, service, serviceName, mutableOps));
                getNodeEngine().getOperationService().execute(partitionThreadRunnable);
                try {
                    partitionThreadRunnable.future.join();
                    operations = postProcessOperations(operations, mutableOps);
                } catch (CompletionException e) {
                    ExceptionUtil.rethrow(e.getCause());
                }
            }
        }
        return operations;
    }

    private Collection<Operation> postProcessOperations(Collection<Operation> previous, Collection<Operation> additional) {
        if (previous.isEmpty()) {
            previous = additional;
        } else if (previous.size() == 1) {
            // immutable singleton list
            previous = new ArrayList<>(previous);
            previous.addAll(additional);
        } else {
            previous.addAll(additional);
        }
        return previous;
    }

    // todo this is ugly
    private void prepareAndAppendReplicationOperationMutable(PartitionReplicationEvent event, ServiceNamespace ns,
                    FragmentedMigrationAwareService service, String serviceName, Collection<Operation> mutableOperations) {

        Operation op = service.prepareReplicationOperation(event, singleton(ns));
        if (op == null) {
            return;
        }

        op.setServiceName(serviceName);
        mutableOperations.add(op);
    }

    private Collection<Operation> prepareAndAppendReplicationOperation(PartitionReplicationEvent event, ServiceNamespace ns,
            FragmentedMigrationAwareService service, String serviceName, Collection<Operation> operations) {

        Operation op = service.prepareReplicationOperation(event, singleton(ns));
        if (op == null) {
            return operations;
        }

        op.setServiceName(serviceName);

        if (operations.isEmpty()) {
            // generally a namespace belongs to a single service only
            operations = singleton(op);
        } else if (operations.size() == 1) {
            operations = new ArrayList<>(operations);
            operations.add(op);
        } else {
            operations.add(op);
        }
        return operations;
    }

    @Override
    public final int getFactoryId() {
        return PartitionDataSerializerHook.F_ID;
    }

    private final class RunOnPartitionThread implements PartitionSpecificRunnable {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        private final Runnable runnable;

        RunOnPartitionThread(Runnable runnable) {
            this.runnable = runnable;
        }

        @Override
        public int getPartitionId() {
            return AbstractPartitionOperation.this.getPartitionId();
        }

        @Override
        public void run() {
            try {
                runnable.run();
                future.complete(null);
            } catch (Exception e) {
                future.completeExceptionally(e);
            }
        }
    }
}
