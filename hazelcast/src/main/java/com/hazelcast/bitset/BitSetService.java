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

package com.hazelcast.bitset;

import com.hazelcast.bitset.impl.BitSetContainer;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.SplitBrainHandlerService;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;

import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class BitSetService implements RemoteService, MigrationAwareService, SplitBrainHandlerService, ManagedService {

    public static final String SERVICE_NAME = "hz:impl:bitset";

    private static final ConstructorFunction<String, BitSetContainer> CONTAINER_CONSTRUCTOR =
            new ConstructorFunction<String, BitSetContainer>() {
                @Override
                public BitSetContainer createNew(String arg) {
                    return new BitSetContainer();
                }
            };

    private final ConcurrentHashMap<String, BitSetContainer> containers = new ConcurrentHashMap<String, BitSetContainer>();

    private volatile NodeEngine nodeEngine;

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        this.nodeEngine = nodeEngine;
    }

    @Override
    public void reset() {

    }

    @Override
    public void shutdown(boolean terminate) {

    }

    @Override
    public DistributedObject createDistributedObject(String objectName) {
        return new BitSetProxy(nodeEngine, this, objectName);
    }

    @Override
    public void destroyDistributedObject(String objectName) {
        containers.remove(objectName);
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        return null;
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent event) {

    }

    @Override
    public void commitMigration(PartitionMigrationEvent event) {

    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {

    }

    @Override
    public Runnable prepareMergeRunnable() {
        return null;
    }

    public BitSetContainer getContainer(String name) {
        return ConcurrencyUtil.getOrPutIfAbsent(containers, name, CONTAINER_CONSTRUCTOR);
    }
}
