/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache.impl.operation;

import com.hazelcast.cache.impl.CacheDataSerializerHook;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.cache.impl.PreJoinCacheConfig;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.Member;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.impl.AbstractNamedOperation;
import com.hazelcast.spi.impl.SimpleExecutionCallback;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Used to create cluster wide cache configuration.
 * <p>
 * This configuration is created:
 * <ul>
 * <li>on local member, when {@code ignoreLocal} is {@code false}</li>
 * <li>other members of the cluster when {@code createAlsoOnOthers} is {@code true} (via sending
 * {@code CacheCreateConfigOperation} operations to other members of the cluster)</li>
 * </ul>
 * <p>
 * Creation of the {@link CacheConfig} on other members of the cluster is prone to races as
 * members join or leave the cluster, so it is not guaranteed that all members of the cluster will
 * be aware of the given {@link CacheConfig} even when {@code createAlsoOnOthers} is {@code true}.
 */
public class CacheCreateConfigOperation
        extends AbstractNamedOperation
        implements IdentifiedDataSerializable {

    private CacheConfig config;
    private boolean createAlsoOnOthers = true;
    private boolean ignoreLocal;

    private boolean returnsResponse = true;

    public CacheCreateConfigOperation() {
    }

    public CacheCreateConfigOperation(CacheConfig config, boolean createAlsoOnOthers) {
        this(config, createAlsoOnOthers, false);
    }

    public CacheCreateConfigOperation(CacheConfig config, boolean createAlsoOnOthers, boolean ignoreLocal) {
        super(config.getNameWithPrefix());
        this.config = config;
        this.createAlsoOnOthers = createAlsoOnOthers;
        this.ignoreLocal = ignoreLocal;
    }

    @Override
    public String getServiceName() {
        return ICacheService.SERVICE_NAME;
    }

    @Override
    public void run() throws Exception {
        ICacheService service = getService();
        if (!ignoreLocal) {
            CacheConfig cacheConfig =
                    config instanceof PreJoinCacheConfig ? ((PreJoinCacheConfig) config).asCacheConfig() : config;
            service.putCacheConfigIfAbsent(cacheConfig);
        }
        if (createAlsoOnOthers) {
            NodeEngine nodeEngine = getNodeEngine();
            Collection<Member> members = nodeEngine.getClusterService().getMembers();
            int remoteNodeCount = members.size() - 1;

            if (remoteNodeCount > 0) {
                postponeReturnResponse();

                ExecutionCallback<Object> callback = new CacheConfigCreateCallback(this, remoteNodeCount);
                OperationService operationService = nodeEngine.getOperationService();
                for (Member member : members) {
                    if (!member.localMember()) {
                        CacheCreateConfigOperation op = new CacheCreateConfigOperation(config, false);
                        operationService
                                .createInvocationBuilder(ICacheService.SERVICE_NAME, op, member.getAddress())
                                .setExecutionCallback(callback)
                                .invoke();
                    }
                }
            }
        }
    }

    private void postponeReturnResponse() {
        // If config already exists or it's local-only created then return response immediately.
        // Otherwise response will be sent after config is created on all members.
        returnsResponse = false;
    }

    @Override
    public void onExecutionFailure(Throwable e) {
        // Execution failed so we should enable `returnsResponse` flag to prevent waiting anymore
        returnsResponse = true;
        super.onExecutionFailure(e);
    }

    private static class CacheConfigCreateCallback extends SimpleExecutionCallback<Object> {

        final AtomicInteger counter;
        final CacheCreateConfigOperation operation;

        public CacheConfigCreateCallback(CacheCreateConfigOperation op, int count) {
            this.operation = op;
            this.counter = new AtomicInteger(count);
        }

        @Override
        public void notify(Object object) {
            if (counter.decrementAndGet() == 0) {
                operation.sendResponse(null);
            }
        }
    }

    @Override
    public Object getResponse() {
        return null;
    }

    @Override
    public boolean returnsResponse() {
        return returnsResponse;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(config);
        out.writeBoolean(createAlsoOnOthers);
        out.writeBoolean(ignoreLocal);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        config = in.readObject();
        createAlsoOnOthers = in.readBoolean();
        ignoreLocal = in.readBoolean();
    }

    @Override
    public int getId() {
        return CacheDataSerializerHook.CREATE_CONFIG;
    }

    @Override
    public int getFactoryId() {
        return CacheDataSerializerHook.F_ID;
    }
}
