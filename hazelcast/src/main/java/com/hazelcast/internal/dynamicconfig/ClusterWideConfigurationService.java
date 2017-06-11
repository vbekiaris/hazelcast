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

package com.hazelcast.internal.dynamicconfig;

import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.CardinalityEstimatorConfig;
import com.hazelcast.config.DurableExecutorConfig;
import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.config.ListConfig;
import com.hazelcast.config.LockConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.config.ReliableTopicConfig;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.ScheduledExecutorConfig;
import com.hazelcast.config.SemaphoreConfig;
import com.hazelcast.config.SetConfig;
import com.hazelcast.config.TopicConfig;
import com.hazelcast.internal.cluster.ClusterVersionListener;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.CoreService;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.version.Version;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.internal.cluster.Versions.V3_8;
import static com.hazelcast.util.InvocationUtil.invokeOnStableCluster;

public class ClusterWideConfigurationService implements MigrationAwareService,
        CoreService, ClusterVersionListener, ManagedService, ConfigurationService {
    public static final String SERVICE_NAME = "configuration-service";
    public static final int CONFIG_PUBLISH_MAX_ATTEMPT_COUNT = 100;
    
    private final DynamicConfigListener listener;

    private NodeEngine nodeEngine;
    private ConcurrentMap<String, MultiMapConfig> multiMapConfigs = new ConcurrentHashMap<String, MultiMapConfig>();
    private ConcurrentMap<String, MapConfig> mapConfigs = new ConcurrentHashMap<String, MapConfig>();
    private ConcurrentMap<String, CardinalityEstimatorConfig> cardinalityEstimatorConfigs =
            new ConcurrentHashMap<String, CardinalityEstimatorConfig>();
    private ConcurrentMap<String, RingbufferConfig> ringbufferConfigs = new ConcurrentHashMap<String, RingbufferConfig>();
    private ConcurrentMap<String, LockConfig> lockConfigs = new ConcurrentHashMap<String, LockConfig>();
    private ConcurrentMap<String, ListConfig> listConfigs = new ConcurrentHashMap<String, ListConfig>();
    private ConcurrentMap<String, SetConfig> setConfigs = new ConcurrentHashMap<String, SetConfig>();
    private ConcurrentMap<String, ReplicatedMapConfig> replicatedMapConfigs =
            new ConcurrentHashMap<String, ReplicatedMapConfig>();
    private ConcurrentMap<String, TopicConfig> topicConfigs = new ConcurrentHashMap<String, TopicConfig>();
    private ConcurrentMap<String, ExecutorConfig> executorConfigs = new ConcurrentHashMap<String, ExecutorConfig>();
    private ConcurrentMap<String, DurableExecutorConfig> durableExecutorConfigs =
            new ConcurrentHashMap<String, DurableExecutorConfig>();
    private ConcurrentMap<String, ScheduledExecutorConfig> scheduledExecutorConfigs =
            new ConcurrentHashMap<String, ScheduledExecutorConfig>();
    private ConcurrentMap<String, SemaphoreConfig> semaphoreConfigs = new ConcurrentHashMap<String, SemaphoreConfig>();
    private ConcurrentMap<String, QueueConfig> queueConfigs = new ConcurrentHashMap<String, QueueConfig>();
    private ConcurrentMap<String, ReliableTopicConfig> reliableTopicConfigs =
            new ConcurrentHashMap<String, ReliableTopicConfig>();
    private ConcurrentMap<String, CacheSimpleConfig> cacheSimpleConfigs =
            new ConcurrentHashMap<String, CacheSimpleConfig>();

    private volatile Version version;

    public ClusterWideConfigurationService(NodeEngine nodeEngine, DynamicConfigListener dynamicConfigListener) {
        this.nodeEngine = nodeEngine;
        this.listener = dynamicConfigListener;
    }

    @Override
    public MultiMapConfig getMultiMapConfig(String name) {
        return multiMapConfigs.get(name);
    }

    @Override
    public MapConfig getMapConfig(String name) {
        return mapConfigs.get(name);
    }

    @Override
    public Map<String, MapConfig> getMapConfigs() {
        return mapConfigs;
    }

    @Override
    public TopicConfig getTopicConfig(String name) {
        return topicConfigs.get(name);
    }

    @Override
    public CardinalityEstimatorConfig getCardinalityEstimatorConfig(String name) {
        return cardinalityEstimatorConfigs.get(name);
    }

    @Override
    public ExecutorConfig getExecutorConfig(String name) {
        return executorConfigs.get(name);
    }

    @Override
    public ScheduledExecutorConfig getScheduledExecutorConfig(String name) {
        return scheduledExecutorConfigs.get(name);
    }

    @Override
    public DurableExecutorConfig getDurableExecutorConfig(String name) {
        return durableExecutorConfigs.get(name);
    }

    @Override
    public SemaphoreConfig getSemaphoreConfig(String name) {
        return semaphoreConfigs.get(name);
    }

    @Override
    public RingbufferConfig getRingbufferConfig(String name) {
        return ringbufferConfigs.get(name);
    }

    @Override
    public LockConfig getLockConfig(String name) {
        return lockConfigs.get(name);
    }

    @Override
    public Map<String, LockConfig> getLockConfigs() {
        return lockConfigs;
    }

    @Override
    public ListConfig getListConfig(String name) {
        return listConfigs.get(name);
    }

    @Override
    public void registerLocally(IdentifiedDataSerializable config) {
        if (config instanceof MultiMapConfig) {
            MultiMapConfig multiMapConfig = (MultiMapConfig) config;
            multiMapConfigs.putIfAbsent(multiMapConfig.getName(), multiMapConfig);
        } else if (config instanceof MapConfig) {
            MapConfig mapConfig = (MapConfig) config;
            MapConfig previousConfig = mapConfigs.putIfAbsent(mapConfig.getName(), mapConfig);
            if (previousConfig == null) {
                listener.onConfigRegistered(mapConfig);
            }
        } else if (config instanceof CardinalityEstimatorConfig) {
            CardinalityEstimatorConfig cardinalityEstimatorConfig = (CardinalityEstimatorConfig) config;
            cardinalityEstimatorConfigs.putIfAbsent(cardinalityEstimatorConfig.getName(), cardinalityEstimatorConfig);
        } else if (config instanceof RingbufferConfig) {
            RingbufferConfig ringbufferConfig = (RingbufferConfig) config;
            ringbufferConfigs.putIfAbsent(ringbufferConfig.getName(), ringbufferConfig);
        } else if (config instanceof LockConfig) {
            LockConfig lockConfig = (LockConfig) config;
            lockConfigs.putIfAbsent(lockConfig.getName(), lockConfig);
        } else if (config instanceof ListConfig) {
            ListConfig listConfig = (ListConfig) config;
            listConfigs.putIfAbsent(listConfig.getName(), listConfig);
        } else if (config instanceof SetConfig) {
            SetConfig setConfig = (SetConfig) config;
            setConfigs.putIfAbsent(setConfig.getName(), setConfig);
        } else if (config instanceof ReplicatedMapConfig) {
            ReplicatedMapConfig replicatedMapConfig = (ReplicatedMapConfig) config;
            replicatedMapConfigs.putIfAbsent(replicatedMapConfig.getName(), replicatedMapConfig);
        } else if (config instanceof TopicConfig) {
            TopicConfig topicConfig = (TopicConfig) config;
            topicConfigs.putIfAbsent(topicConfig.getName(), topicConfig);
        } else if (config instanceof ExecutorConfig) {
            ExecutorConfig executorConfig = (ExecutorConfig) config;
            executorConfigs.putIfAbsent(executorConfig.getName(), executorConfig);
        } else if (config instanceof DurableExecutorConfig) {
            DurableExecutorConfig durableExecutorConfig = (DurableExecutorConfig) config;
            durableExecutorConfigs.putIfAbsent(durableExecutorConfig.getName(), durableExecutorConfig);
        } else if (config instanceof ScheduledExecutorConfig) {
            ScheduledExecutorConfig scheduledExecutorConfig = (ScheduledExecutorConfig) config;
            scheduledExecutorConfigs.putIfAbsent(scheduledExecutorConfig.getName(), scheduledExecutorConfig);
        } else if (config instanceof QueueConfig) {
            QueueConfig queueConfig = (QueueConfig) config;
            queueConfigs.putIfAbsent(queueConfig.getName(), queueConfig);
        } else if (config instanceof ReliableTopicConfig) {
            ReliableTopicConfig reliableTopicConfig = (ReliableTopicConfig) config;
            reliableTopicConfigs.putIfAbsent(reliableTopicConfig.getName(), reliableTopicConfig);
        } else if (config instanceof CacheSimpleConfig) {
            CacheSimpleConfig cacheSimpleConfig = (CacheSimpleConfig) config;
            CacheSimpleConfig previous = cacheSimpleConfigs.putIfAbsent(cacheSimpleConfig.getName(), cacheSimpleConfig);
            if (previous == null) {
                listener.onConfigRegistered(cacheSimpleConfig);
            }
        } else {
            throw new UnsupportedOperationException("Unsupported config type: " + config);
        }
    }

    @Override
    public void broadcastConfig(IdentifiedDataSerializable config) {
        invokeOnStableCluster(nodeEngine, new AddDynamicConfigOperationFactory(config), null,
                CONFIG_PUBLISH_MAX_ATTEMPT_COUNT);
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        if (version.isLessOrEqual(V3_8)) {
            return null;
        }
        return new DynamicConfigReplicationOperation(multiMapConfigs, mapConfigs);
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent event) {
        //no-op
    }

    @Override
    public void commitMigration(PartitionMigrationEvent event) {
        //no-op
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {
        //no-op
    }

    @Override
    public void onClusterVersionChange(Version newVersion) {
        version = newVersion;
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        listener.onServiceInitialized(this);
    }

    @Override
    public void reset() {
        multiMapConfigs.clear();
    }

    @Override
    public void shutdown(boolean terminate) {
        //no-op
    }

    @Override
    public QueueConfig getQueueConfig(String name) {
        return queueConfigs.get(name);
    }

    @Override
    public Map<String, QueueConfig> getQueueConfigs() {
        return queueConfigs;
    }

    @Override
    public ConcurrentMap<String, ListConfig> getListConfigs() {
        return listConfigs;
    }

    @Override
    public SetConfig getSetConfig(String name) {
        return setConfigs.get(name);
    }

    @Override
    public ConcurrentMap<String, SetConfig> getSetConfigs() {
        return setConfigs;
    }

    @Override
    public ConcurrentMap<String, MultiMapConfig> getMultiMapConfigs() {
        return multiMapConfigs;
    }

    @Override
    public ReplicatedMapConfig getReplicatedMapConfig(String name) {
        return replicatedMapConfigs.get(name);
    }

    @Override
    public ConcurrentMap<String, ReplicatedMapConfig> getReplicatedMapConfigs() {
        return replicatedMapConfigs;
    }

    @Override
    public ConcurrentMap<String, RingbufferConfig> getRingbufferConfigs() {
        return ringbufferConfigs;
    }

    @Override
    public ConcurrentMap<String, TopicConfig> getTopicConfigs() {
        return topicConfigs;
    }

    @Override
    public ReliableTopicConfig getReliableTopicConfig(String name) {
        return reliableTopicConfigs.get(name);
    }

    @Override
    public ConcurrentMap<String, ReliableTopicConfig> getReliableTopicConfigs() {
        return reliableTopicConfigs;
    }

    @Override
    public ConcurrentMap<String, ExecutorConfig> getExecutorConfigs() {
        return executorConfigs;
    }

    @Override
    public ConcurrentMap<String, DurableExecutorConfig> getDurableExecutorConfigs() {
        return durableExecutorConfigs;
    }

    @Override
    public ConcurrentMap<String, ScheduledExecutorConfig> getScheduledExecutorConfigs() {
        return scheduledExecutorConfigs;
    }

    @Override
    public ConcurrentMap<String, CardinalityEstimatorConfig> getCardinalityEstimatorConfigs() {
        return cardinalityEstimatorConfigs;
    }

    @Override
    public ConcurrentMap<String, SemaphoreConfig> getSemaphoreConfigs() {
        return semaphoreConfigs;
    }

    @Override
    public CacheSimpleConfig getCacheConfig(String name) {
        return cacheSimpleConfigs.get(name);
    }

    @Override
    public Map<String, CacheSimpleConfig> getCacheSimpleConfigs() {
        return cacheSimpleConfigs;
    }
}
