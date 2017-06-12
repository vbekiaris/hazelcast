/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.task.dynamicconfig;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.DynamicConfigAddMapConfigCodec;
import com.hazelcast.client.impl.protocol.codec.DynamicConfigAddTopicConfigCodec;
import com.hazelcast.config.CacheDeserializedValues;
import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapPartitionLostListenerConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.config.PartitioningStrategyConfig;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.config.TopicConfig;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.dynamicconfig.AddDynamicConfigOperationFactory;
import com.hazelcast.map.eviction.MapEvictionPolicy;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.OperationFactory;

import java.util.ArrayList;
import java.util.List;

public class AddMapConfigMessageTask
        extends AbstractAddConfigMessageTask<DynamicConfigAddMapConfigCodec.RequestParameters> {

    public AddMapConfigMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected DynamicConfigAddMapConfigCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return DynamicConfigAddMapConfigCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return DynamicConfigAddMapConfigCodec.encodeResponse();
    }

    @Override
    protected OperationFactory getOperationFactory() {
        MapConfig config = new MapConfig(parameters.name);
        config.setAsyncBackupCount(parameters.asyncBackupCount);
        config.setBackupCount(parameters.backupCount);
        config.setCacheDeserializedValues(CacheDeserializedValues.valueOf(parameters.cacheDeserializedValues));
        config.setEvictionPolicy(EvictionPolicy.valueOf(parameters.evictionPolicy));
        if (parameters.listenerConfigs != null && !parameters.listenerConfigs.isEmpty()) {
            config.setEntryListenerConfigs(
                    (List<EntryListenerConfig>) adaptListenerConfigs(parameters.listenerConfigs));
        }
        config.setHotRestartConfig(parameters.hotRestartConfig);
        config.setInMemoryFormat(InMemoryFormat.valueOf(parameters.inMemoryFormat));
        config.setMapAttributeConfigs(parameters.mapAttributeConfigs);
        if (parameters.mapEvictionPolicy != null) {
            MapEvictionPolicy evictionPolicy = serializationService.toObject(parameters.mapEvictionPolicy);
            config.setMapEvictionPolicy(evictionPolicy);
        }
        config.setMapIndexConfigs(parameters.mapIndexConfigs);
        if (parameters.mapStoreConfig != null) {
            config.setMapStoreConfig(parameters.mapStoreConfig.asMapStoreConfig(serializationService));
        }
        config.setMaxIdleSeconds(parameters.maxIdleSeconds);
        config.setMaxSizeConfig(new MaxSizeConfig(parameters.maxSizeConfigSize,
                MaxSizeConfig.MaxSizePolicy.valueOf(parameters.maxSizeConfigMaxSizePolicy)));
        config.setMergePolicy(parameters.mergePolicy);
        config.setNearCacheConfig(parameters.nearCacheConfig.asNearCacheConfig(serializationService));
        config.setPartitioningStrategyConfig(getPartitioningStrategyConfig());
        if (parameters.partitionLostListenerConfigs != null && !parameters.partitionLostListenerConfigs.isEmpty()) {
            config.setPartitionLostListenerConfigs(
                    (List<MapPartitionLostListenerConfig>) adaptListenerConfigs(parameters.partitionLostListenerConfigs));
        }
        config.setQuorumName(parameters.quorumName);
        if (parameters.queryCacheConfigs != null && !parameters.queryCacheConfigs.isEmpty()) {
            List<QueryCacheConfig> queryCacheConfigs = new ArrayList<QueryCacheConfig>(parameters.queryCacheConfigs.size());
            for (QueryCacheConfigHolder holder : parameters.queryCacheConfigs) {
                queryCacheConfigs.add(holder.asQueryCacheConfig(serializationService));
            }
        }
        return new AddDynamicConfigOperationFactory(config);
    }

    private PartitioningStrategyConfig getPartitioningStrategyConfig() {
        if (parameters.partitioningStrategyClassName != null) {
            return new PartitioningStrategyConfig(parameters.partitioningStrategyClassName);
        } else if (parameters.partitioningStrategyImplementation != null) {
            PartitioningStrategy partitioningStrategy =
                    serializationService.toObject(parameters.partitioningStrategyImplementation);
            return new PartitioningStrategyConfig(partitioningStrategy);
        } else {
            return null;
        }
    }
}
