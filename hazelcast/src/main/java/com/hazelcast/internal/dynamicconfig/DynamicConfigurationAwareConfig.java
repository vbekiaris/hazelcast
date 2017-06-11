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
import com.hazelcast.config.Config;
import com.hazelcast.config.ConfigPatternMatcher;
import com.hazelcast.config.DurableExecutorConfig;
import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.HotRestartPersistenceConfig;
import com.hazelcast.config.JobTrackerConfig;
import com.hazelcast.config.ListConfig;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.LockConfig;
import com.hazelcast.config.ManagementCenterConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MemberAttributeConfig;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.PartitionGroupConfig;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.config.QuorumConfig;
import com.hazelcast.config.ReliableTopicConfig;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.ScheduledExecutorConfig;
import com.hazelcast.config.SecurityConfig;
import com.hazelcast.config.SemaphoreConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.ServicesConfig;
import com.hazelcast.config.SetConfig;
import com.hazelcast.config.TopicConfig;
import com.hazelcast.config.UserCodeDeploymentConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.core.ManagedContext;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.internal.dynamicconfig.AggregatingMap.aggregate;

public class DynamicConfigurationAwareConfig extends Config {
    private final Config staticConfig;
    private ConfigurationService configurationService = new EmptyConfigurationService();

    public DynamicConfigurationAwareConfig(Config staticConfig) {
        this.staticConfig = staticConfig;
    }

    @Override
    public ClassLoader getClassLoader() {
        return staticConfig.getClassLoader();
    }

    @Override
    public Config setClassLoader(ClassLoader classLoader) {
        return staticConfig.setClassLoader(classLoader);
    }

    @Override
    public ConfigPatternMatcher getConfigPatternMatcher() {
        return staticConfig.getConfigPatternMatcher();
    }

    @Override
    public void setConfigPatternMatcher(ConfigPatternMatcher configPatternMatcher) {
        staticConfig.setConfigPatternMatcher(configPatternMatcher);
    }

    @Override
    public String getProperty(String name) {
        return staticConfig.getProperty(name);
    }

    @Override
    public Config setProperty(String name, String value) {
        return staticConfig.setProperty(name, value);
    }

    @Override
    public MemberAttributeConfig getMemberAttributeConfig() {
        return staticConfig.getMemberAttributeConfig();
    }

    @Override
    public void setMemberAttributeConfig(MemberAttributeConfig memberAttributeConfig) {
        staticConfig.setMemberAttributeConfig(memberAttributeConfig);
    }

    @Override
    public Properties getProperties() {
        return staticConfig.getProperties();
    }

    @Override
    public Config setProperties(Properties properties) {
        return staticConfig.setProperties(properties);
    }

    @Override
    public String getInstanceName() {
        return staticConfig.getInstanceName();
    }

    @Override
    public Config setInstanceName(String instanceName) {
        return staticConfig.setInstanceName(instanceName);
    }

    @Override
    public GroupConfig getGroupConfig() {
        return staticConfig.getGroupConfig();
    }

    @Override
    public Config setGroupConfig(GroupConfig groupConfig) {
        return staticConfig.setGroupConfig(groupConfig);
    }

    @Override
    public NetworkConfig getNetworkConfig() {
        return staticConfig.getNetworkConfig();
    }

    @Override
    public Config setNetworkConfig(NetworkConfig networkConfig) {
        return staticConfig.setNetworkConfig(networkConfig);
    }

    @Override
    public MapConfig findMapConfig(String name) {
        return getMapConfigInternal(name, "default").getAsReadOnly();
    }

    @Override
    public MapConfig getMapConfig(String name) {
        return getMapConfigInternal(name, name);
    }

    private MapConfig getMapConfigInternal(String name, String fallbackName) {
        Map<String, MapConfig> staticMapConfigs = staticConfig.getMapConfigs();
        MapConfig mapConfig = Config.lookupByPattern(staticMapConfigs, name);
        if (mapConfig == null) {
            mapConfig = configurationService.getMapConfig(name);
        }
        if (mapConfig == null) {
            mapConfig = staticConfig.getMapConfig(fallbackName);
        }
        return mapConfig;
    }

    @Override
    public Config addMapConfig(MapConfig mapConfig) {
        configurationService.broadcastConfig(mapConfig);
        return this;
    }

    @Override
    public Map<String, MapConfig> getMapConfigs() {
        Map<String, MapConfig> staticMapConfigs = staticConfig.getMapConfigs();
        Map<String, MapConfig> dynamicMapConfigs = configurationService.getMapConfigs();
        return aggregate(staticMapConfigs, dynamicMapConfigs);
    }

    @Override
    public Config setMapConfigs(Map<String, MapConfig> mapConfigs) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public CacheSimpleConfig findCacheConfig(String name) {
        //intentional: as of Hazelcast 3.x we do not use default for JCache!
        CacheSimpleConfig cacheConfig = getCacheConfigInternal(name, null);
        if (cacheConfig == null) {
            return null;
        }
        return cacheConfig.getAsReadOnly();
    }

    @Override
    public CacheSimpleConfig getCacheConfig(String name) {
        return getCacheConfigInternal(name, name);
    }

    private CacheSimpleConfig getCacheConfigInternal(String name, String fallbackName) {
        Map<String, CacheSimpleConfig> staticCacheConfigs = staticConfig.getCacheConfigs();
        CacheSimpleConfig cacheSimpleConfig = Config.lookupByPattern(staticCacheConfigs, name);
        if (cacheSimpleConfig == null) {
            cacheSimpleConfig = configurationService.getCacheConfig(name);
        }
        if (cacheSimpleConfig == null && fallbackName != null) {
            cacheSimpleConfig = staticConfig.getCacheConfig(fallbackName);
        }
        return cacheSimpleConfig;
    }

    @Override
    public Config addCacheConfig(CacheSimpleConfig cacheConfig) {
        configurationService.broadcastConfig(cacheConfig);
        return this;
    }

    @Override
    public Map<String, CacheSimpleConfig> getCacheConfigs() {
        Map<String, CacheSimpleConfig> staticConfigs = staticConfig.getCacheConfigs();
        Map<String, CacheSimpleConfig> dynamicConfigs = configurationService.getCacheSimpleConfigs();

        return aggregate(staticConfigs, dynamicConfigs);
    }

    @Override
    public Config setCacheConfigs(Map<String, CacheSimpleConfig> cacheConfigs) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public QueueConfig findQueueConfig(String name) {
        return getQueueConfigInternal(name, "default").getAsReadOnly();
    }

    @Override
    public QueueConfig getQueueConfig(String name) {
        return getQueueConfigInternal(name, name);
    }

    private QueueConfig getQueueConfigInternal(String name, String fallbackName) {
        Map<String, QueueConfig> staticQueueConfigs = staticConfig.getQueueConfigs();
        QueueConfig queueConfig = Config.lookupByPattern(staticQueueConfigs, name);
        if (queueConfig == null) {
            queueConfig = configurationService.getQueueConfig(name);
        }
        if (queueConfig == null) {
            queueConfig = staticConfig.getQueueConfig(fallbackName);
        }
        return queueConfig;
    }

    @Override
    public Config addQueueConfig(QueueConfig queueConfig) {
        configurationService.broadcastConfig(queueConfig);
        return this;
    }

    @Override
    public Map<String, QueueConfig> getQueueConfigs() {
        Map<String, QueueConfig> staticQueueConfigs = staticConfig.getQueueConfigs();
        Map<String, QueueConfig> dynamicQueueConfigs = configurationService.getQueueConfigs();
        return aggregate(staticQueueConfigs, dynamicQueueConfigs);
    }

    @Override
    public Config setQueueConfigs(Map<String, QueueConfig> queueConfigs) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public LockConfig findLockConfig(String name) {
        return getLockConfigInternal(name, "default").getAsReadOnly();
    }

    @Override
    public LockConfig getLockConfig(String name) {
        return getLockConfigInternal(name, name);
    }

    private LockConfig getLockConfigInternal(String name, String fallbackName) {
        Map<String, LockConfig> staticLockConfigs = staticConfig.getLockConfigs();
        LockConfig lockConfig = Config.lookupByPattern(staticLockConfigs, name);
        if (lockConfig == null) {
            lockConfig = configurationService.getLockConfig(name);
        }
        if (lockConfig == null) {
            lockConfig = staticConfig.getLockConfig(fallbackName);
        }
        return lockConfig;
    }

    @Override
    public Config addLockConfig(LockConfig lockConfig) {
        configurationService.broadcastConfig(lockConfig);
        return this;
    }

    @Override
    public Map<String, LockConfig> getLockConfigs() {
        Map<String, LockConfig> staticLockConfigs = staticConfig.getLockConfigs();
        Map<String, LockConfig> dynamiclockConfigs = configurationService.getLockConfigs();
        return aggregate(staticLockConfigs, dynamiclockConfigs);
    }

    @Override
    public Config setLockConfigs(Map<String, LockConfig> lockConfigs) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public ListConfig findListConfig(String name) {
        return getListConfigInternal(name, "default").getAsReadOnly();
    }

    @Override
    public ListConfig getListConfig(String name) {
        return getListConfigInternal(name, name);
    }

    private ListConfig getListConfigInternal(String name, String fallbackName) {
        Map<String, ListConfig> staticListConfigs = staticConfig.getListConfigs();
        ListConfig listConfig = Config.lookupByPattern(staticListConfigs, name);
        if (listConfig == null) {
            listConfig = configurationService.getListConfig(name);
        }
        if (listConfig == null) {
            listConfig = staticConfig.getListConfig(fallbackName);
        }
        return listConfig;
    }

    @Override
    public Config addListConfig(ListConfig listConfig) {
        configurationService.broadcastConfig(listConfig);
        return this;
    }

    @Override
    public Map<String, ListConfig> getListConfigs() {
        Map<String, ListConfig> staticListConfigs = staticConfig.getListConfigs();
        Map<String, ListConfig> dynamicListConfigs = configurationService.getListConfigs();

        return aggregate(staticListConfigs, dynamicListConfigs);
    }

    @Override
    public Config setListConfigs(Map<String, ListConfig> listConfigs) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public SetConfig findSetConfig(String name) {
        return getSetConfigInternal(name, "default").getAsReadOnly();
    }

    @Override
    public SetConfig getSetConfig(String name) {
        return getSetConfigInternal(name, name);
    }

    private SetConfig getSetConfigInternal(String name, String fallbackName) {
        Map<String, SetConfig> staticSetConfigs = staticConfig.getSetConfigs();
        SetConfig setConfig = Config.lookupByPattern(staticSetConfigs, name);
        if (setConfig == null) {
            setConfig = configurationService.getSetConfig(name);
        }
        if (setConfig == null) {
            setConfig = staticConfig.getSetConfig(fallbackName);
        }
        return setConfig;
    }

    @Override
    public Config addSetConfig(SetConfig setConfig) {
        configurationService.broadcastConfig(setConfig);
        return this;
    }

    @Override
    public Map<String, SetConfig> getSetConfigs() {
        Map<String, SetConfig> staticSetConfigs = staticConfig.getSetConfigs();
        Map<String, SetConfig> dynamicSetConfigs = configurationService.getSetConfigs();
        return aggregate(staticSetConfigs, dynamicSetConfigs);
    }

    @Override
    public Config setSetConfigs(Map<String, SetConfig> setConfigs) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public MultiMapConfig findMultiMapConfig(String name) {
        return getMultiMapConfigInternal(name, "default").getAsReadOnly();
    }

    @Override
    public MultiMapConfig getMultiMapConfig(String name) {
        return getMultiMapConfigInternal(name, name);
    }

    private MultiMapConfig getMultiMapConfigInternal(String name, String fallbackName) {
        Map<String, MultiMapConfig> staticMultiMapConfigs = staticConfig.getMultiMapConfigs();
        MultiMapConfig multiMapConfig = Config.lookupByPattern(staticMultiMapConfigs, name);
        if (multiMapConfig == null) {
            multiMapConfig = configurationService.getMultiMapConfig(name);
        }
        if (multiMapConfig == null) {
            multiMapConfig = staticConfig.getMultiMapConfig(fallbackName);
        }
        return multiMapConfig;
    }


    @Override
    public Config addMultiMapConfig(MultiMapConfig multiMapConfig) {
        configurationService.broadcastConfig(multiMapConfig);
        return this;
    }

    @Override
    public Map<String, MultiMapConfig> getMultiMapConfigs() {
        Map<String, MultiMapConfig> staticConfigs = staticConfig.getMultiMapConfigs();
        Map<String, MultiMapConfig> dynamicConfigs = configurationService.getMultiMapConfigs();

        return aggregate(staticConfigs, dynamicConfigs);
    }

    @Override
    public Config setMultiMapConfigs(Map<String, MultiMapConfig> multiMapConfigs) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public ReplicatedMapConfig findReplicatedMapConfig(String name) {
        return getReplicatedMapConfigInternal(name, "default").getAsReadOnly();
    }

    @Override
    public ReplicatedMapConfig getReplicatedMapConfig(String name) {
        return getReplicatedMapConfigInternal(name, name);
    }

    private ReplicatedMapConfig getReplicatedMapConfigInternal(String name, String fallbackName) {
        Map<String, ReplicatedMapConfig> replicatedMapConfigs = staticConfig.getReplicatedMapConfigs();
        ReplicatedMapConfig replicatedMapConfig = Config.lookupByPattern(replicatedMapConfigs, name);
        if (replicatedMapConfig == null) {
            replicatedMapConfig = configurationService.getReplicatedMapConfig(name);
        }
        if (replicatedMapConfig == null) {
            replicatedMapConfig = staticConfig.getReplicatedMapConfig(fallbackName);
        }
        return replicatedMapConfig;
    }

    @Override
    public Config addReplicatedMapConfig(ReplicatedMapConfig replicatedMapConfig) {
        configurationService.broadcastConfig(replicatedMapConfig);
        return this;
    }

    @Override
    public Map<String, ReplicatedMapConfig> getReplicatedMapConfigs() {
        Map<String, ReplicatedMapConfig> staticConfigs = staticConfig.getReplicatedMapConfigs();
        Map<String, ReplicatedMapConfig> dynamicConfigs = configurationService.getReplicatedMapConfigs();

        return aggregate(staticConfigs, dynamicConfigs);
    }

    @Override
    public Config setReplicatedMapConfigs(Map<String, ReplicatedMapConfig> replicatedMapConfigs) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public RingbufferConfig findRingbufferConfig(String name) {
        return getRingbufferConfigInternal(name, "default").getAsReadOnly();
    }

    @Override
    public RingbufferConfig getRingbufferConfig(String name) {
        return getRingbufferConfigInternal(name, name);
    }

    private RingbufferConfig getRingbufferConfigInternal(String name, String fallbackName) {
        Map<String, RingbufferConfig> ringbufferConfigs = staticConfig.getRingbufferConfigs();
        RingbufferConfig ringbufferConfig = Config.lookupByPattern(ringbufferConfigs, name);
        if (ringbufferConfig == null) {
            ringbufferConfig = configurationService.getRingbufferConfig(name);
        }
        if (ringbufferConfig == null) {
            ringbufferConfig = staticConfig.getRingbufferConfig(fallbackName);
        }
        return ringbufferConfig;
    }

    @Override
    public Config addRingBufferConfig(RingbufferConfig ringbufferConfig) {
        configurationService.broadcastConfig(ringbufferConfig);
        return this;
    }

    @Override
    public Map<String, RingbufferConfig> getRingbufferConfigs() {
        Map<String, RingbufferConfig> staticConfigs = staticConfig.getRingbufferConfigs();
        Map<String, RingbufferConfig> dynamicConfigs = configurationService.getRingbufferConfigs();

        return aggregate(staticConfigs, dynamicConfigs);
    }

    @Override
    public Config setRingbufferConfigs(Map<String, RingbufferConfig> ringbufferConfigs) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public TopicConfig findTopicConfig(String name) {
        return getTopicConfigInternal(name, "default").getAsReadOnly();
    }

    @Override
    public TopicConfig getTopicConfig(String name) {
        return getTopicConfigInternal(name, name);
    }

    private TopicConfig getTopicConfigInternal(String name, String fallbackName) {
        Map<String, TopicConfig> topicConfigs = staticConfig.getTopicConfigs();
        TopicConfig topicConfig = Config.lookupByPattern(topicConfigs, name);
        if (topicConfig == null) {
            topicConfig = configurationService.getTopicConfig(name);
        }
        if (topicConfig == null) {
            topicConfig = staticConfig.getTopicConfig(fallbackName);
        }
        return topicConfig;
    }

    @Override
    public Config addTopicConfig(TopicConfig topicConfig) {
        configurationService.broadcastConfig(topicConfig);
        return this;
    }

    @Override
    public Map<String, TopicConfig> getTopicConfigs() {
        Map<String, TopicConfig> staticConfigs = staticConfig.getTopicConfigs();
        Map<String, TopicConfig> dynamicConfigs = configurationService.getTopicConfigs();

        return aggregate(staticConfigs, dynamicConfigs);
    }

    @Override
    public Config setTopicConfigs(Map<String, TopicConfig> mapTopicConfigs) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public ReliableTopicConfig findReliableTopicConfig(String name) {
        return getReliableTopicConfigInternal(name, "default").getAsReadOnly();
    }

    @Override
    public ReliableTopicConfig getReliableTopicConfig(String name) {
        return getReliableTopicConfigInternal(name, name);
    }

    private ReliableTopicConfig getReliableTopicConfigInternal(String name, String fallbackName) {
        Map<String, ReliableTopicConfig> reliableTopicConfigs = staticConfig.getReliableTopicConfigs();
        ReliableTopicConfig reliableTopicConfig = Config.lookupByPattern(reliableTopicConfigs, name);
        if (reliableTopicConfig == null) {
            reliableTopicConfig = configurationService.getReliableTopicConfig(name);
        }
        if (reliableTopicConfig == null) {
            reliableTopicConfig = staticConfig.getReliableTopicConfig(fallbackName);
        }
        return reliableTopicConfig;
    }

    @Override
    public Map<String, ReliableTopicConfig> getReliableTopicConfigs() {
        Map<String, ReliableTopicConfig> staticConfigs = staticConfig.getReliableTopicConfigs();
        Map<String, ReliableTopicConfig> dynamicConfigs = configurationService.getReliableTopicConfigs();

        return aggregate(staticConfigs, dynamicConfigs);
    }

    @Override
    public Config addReliableTopicConfig(ReliableTopicConfig reliableTopicConfig) {
        configurationService.broadcastConfig(reliableTopicConfig);
        return this;
    }

    @Override
    public Config setReliableTopicConfigs(Map<String, ReliableTopicConfig> reliableTopicConfigs) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public ExecutorConfig findExecutorConfig(String name) {
        return getExecutorConfigInternal(name, "default").getAsReadOnly();
    }

    @Override
    public ExecutorConfig getExecutorConfig(String name) {
        return getExecutorConfigInternal(name, name);
    }

    private ExecutorConfig getExecutorConfigInternal(String name, String fallbackName) {
        Map<String, ExecutorConfig> executorConfigs = staticConfig.getExecutorConfigs();
        ExecutorConfig executorConfig = Config.lookupByPattern(executorConfigs, name);
        if (executorConfig == null) {
            executorConfig = configurationService.getExecutorConfig(name);
        }
        if (executorConfig == null) {
            executorConfig = staticConfig.getExecutorConfig(fallbackName);
        }
        return executorConfig;
    }

    @Override
    public Config addExecutorConfig(ExecutorConfig executorConfig) {
        configurationService.broadcastConfig(executorConfig);
        return this;
    }

    @Override
    public Map<String, ExecutorConfig> getExecutorConfigs() {
        Map<String, ExecutorConfig> staticConfigs = staticConfig.getExecutorConfigs();
        Map<String, ExecutorConfig> dynamicConfigs = configurationService.getExecutorConfigs();

        return aggregate(staticConfigs, dynamicConfigs);
    }

    @Override
    public Config setExecutorConfigs(Map<String, ExecutorConfig> executorConfigs) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public DurableExecutorConfig findDurableExecutorConfig(String name) {
        return getDurableExecutorConfigInternal(name, "default");
    }

    @Override
    public DurableExecutorConfig getDurableExecutorConfig(String name) {
        return getDurableExecutorConfigInternal(name, name);
    }

    private DurableExecutorConfig getDurableExecutorConfigInternal(String name, String fallbackName) {
        Map<String, DurableExecutorConfig> durableExecutorConfigs = staticConfig.getDurableExecutorConfigs();
        DurableExecutorConfig durableExecutorConfig = Config.lookupByPattern(durableExecutorConfigs, name);
        if (durableExecutorConfig == null) {
            durableExecutorConfig = configurationService.getDurableExecutorConfig(name);
        }
        if (durableExecutorConfig == null) {
            durableExecutorConfig = staticConfig.getDurableExecutorConfig(fallbackName);
        }
        return durableExecutorConfig;
    }

    @Override
    public Config addDurableExecutorConfig(DurableExecutorConfig durableExecutorConfig) {
        configurationService.broadcastConfig(durableExecutorConfig);
        return this;
    }

    @Override
    public Map<String, DurableExecutorConfig> getDurableExecutorConfigs() {
        Map<String, DurableExecutorConfig> staticConfigs = staticConfig.getDurableExecutorConfigs();
        Map<String, DurableExecutorConfig> dynamicConfigs = configurationService.getDurableExecutorConfigs();

        return aggregate(staticConfigs, dynamicConfigs);
    }

    @Override
    public Config setDurableExecutorConfigs(Map<String, DurableExecutorConfig> durableExecutorConfigs) {
        throw new UnsupportedOperationException("Unsupported operation");
    }


    @Override
    public ScheduledExecutorConfig findScheduledExecutorConfig(String name) {
        return getScheduledExecutorConfigInternal(name, "default");
    }

    @Override
    public ScheduledExecutorConfig getScheduledExecutorConfig(String name) {
        return getScheduledExecutorConfigInternal(name, name);
    }

    private ScheduledExecutorConfig getScheduledExecutorConfigInternal(String name, String fallbackName) {
        Map<String, ScheduledExecutorConfig> scheduledExecutorConfigs = staticConfig.getScheduledExecutorConfigs();
        ScheduledExecutorConfig scheduledExecutorConfig = Config.lookupByPattern(scheduledExecutorConfigs, name);
        if (scheduledExecutorConfig == null) {
            scheduledExecutorConfig = configurationService.getScheduledExecutorConfig(name);
        }
        if (scheduledExecutorConfig == null) {
            scheduledExecutorConfig = staticConfig.getScheduledExecutorConfig(fallbackName);
        }
        return scheduledExecutorConfig;
    }

    @Override
    public Map<String, ScheduledExecutorConfig> getScheduledExecutorConfigs() {
        Map<String, ScheduledExecutorConfig> staticConfigs = staticConfig.getScheduledExecutorConfigs();
        Map<String, ScheduledExecutorConfig> dynamicConfigs = configurationService.getScheduledExecutorConfigs();

        return aggregate(staticConfigs, dynamicConfigs);
    }

    @Override
    public Config addScheduledExecutorConfig(ScheduledExecutorConfig scheduledExecutorConfig) {
        configurationService.broadcastConfig(scheduledExecutorConfig);
        return this;
    }

    @Override
    public Config setScheduledExecutorConfigs(Map<String, ScheduledExecutorConfig> scheduledExecutorConfigs) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public CardinalityEstimatorConfig findCardinalityEstimatorConfig(String name) {
        return getCardinalityEstimatorConfigInternal(name, "default");
    }

    @Override
    public CardinalityEstimatorConfig getCardinalityEstimatorConfig(String name) {
        return getCardinalityEstimatorConfigInternal(name, name);
    }

    private CardinalityEstimatorConfig getCardinalityEstimatorConfigInternal(String name, String fallbackName) {
        Map<String, CardinalityEstimatorConfig> cardinalityEstimatorConfigs = staticConfig.getCardinalityEstimatorConfigs();
        CardinalityEstimatorConfig cardinalityEstimatorConfig = Config.lookupByPattern(cardinalityEstimatorConfigs, name);
        if (cardinalityEstimatorConfig == null) {
            cardinalityEstimatorConfig = configurationService.getCardinalityEstimatorConfig(name);
        }
        if (cardinalityEstimatorConfig == null) {
            cardinalityEstimatorConfig = staticConfig.getCardinalityEstimatorConfig(fallbackName);
        }
        return cardinalityEstimatorConfig;
    }


    @Override
    public Config addCardinalityEstimatorConfig(CardinalityEstimatorConfig cardinalityEstimatorConfig) {
        configurationService.broadcastConfig(cardinalityEstimatorConfig);
        return this;
    }

    @Override
    public Map<String, CardinalityEstimatorConfig> getCardinalityEstimatorConfigs() {
        Map<String, CardinalityEstimatorConfig> staticConfigs = staticConfig.getCardinalityEstimatorConfigs();
        Map<String, CardinalityEstimatorConfig> dynamicConfigs = configurationService.getCardinalityEstimatorConfigs();

        return aggregate(staticConfigs, dynamicConfigs);
    }

    @Override
    public Config setCardinalityEstimatorConfigs(Map<String, CardinalityEstimatorConfig> cardinalityEstimatorConfigs) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public SemaphoreConfig findSemaphoreConfig(String name) {
        return getSemaphoreConfigInternal(name, "default").getAsReadOnly();
    }

    @Override
    public SemaphoreConfig getSemaphoreConfig(String name) {
        return getSemaphoreConfigInternal(name, name);
    }

    private SemaphoreConfig getSemaphoreConfigInternal(String name, String fallbackName) {
        Map<String, SemaphoreConfig> semaphoreConfigs = staticConfig.getSemaphoreConfigsAsMap();
        SemaphoreConfig semaphoreConfig = Config.lookupByPattern(semaphoreConfigs, name);
        if (semaphoreConfig == null) {
            semaphoreConfig = configurationService.getSemaphoreConfig(name);
        }
        if (semaphoreConfig == null) {
            semaphoreConfig = staticConfig.getSemaphoreConfig(fallbackName);
        }
        return semaphoreConfig;
    }


    @Override
    public Config addSemaphoreConfig(SemaphoreConfig semaphoreConfig) {
        configurationService.broadcastConfig(semaphoreConfig);
        return this;
    }

    @Override
    public Collection<SemaphoreConfig> getSemaphoreConfigs() {
        Collection<SemaphoreConfig> staticConfigs = staticConfig.getSemaphoreConfigs();
        Map<String, SemaphoreConfig> semaphoreConfigs = configurationService.getSemaphoreConfigs();

        ArrayList<SemaphoreConfig> aggregated = new ArrayList<SemaphoreConfig>(staticConfigs);
        aggregated.addAll(semaphoreConfigs.values());

        return aggregated;
    }

    @Override
    public Map<String, SemaphoreConfig> getSemaphoreConfigsAsMap() {
        Map<String, SemaphoreConfig> staticConfigs = staticConfig.getSemaphoreConfigsAsMap();
        Map<String, SemaphoreConfig> dynamicConfigs = configurationService.getSemaphoreConfigs();

        return aggregate(staticConfigs, dynamicConfigs);
    }

    @Override
    public Config setSemaphoreConfigs(Map<String, SemaphoreConfig> semaphoreConfigs) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public WanReplicationConfig getWanReplicationConfig(String name) {
        return staticConfig.getWanReplicationConfig(name);
    }

    @Override
    public Config addWanReplicationConfig(WanReplicationConfig wanReplicationConfig) {
        return staticConfig.addWanReplicationConfig(wanReplicationConfig);
    }

    @Override
    public Map<String, WanReplicationConfig> getWanReplicationConfigs() {
        return staticConfig.getWanReplicationConfigs();
    }

    @Override
    public Config setWanReplicationConfigs(Map<String, WanReplicationConfig> wanReplicationConfigs) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public JobTrackerConfig findJobTrackerConfig(String name) {
        return staticConfig.findJobTrackerConfig(name);
    }

    @Override
    public JobTrackerConfig getJobTrackerConfig(String name) {
        return staticConfig.getJobTrackerConfig(name);
    }

    @Override
    public Config addJobTrackerConfig(JobTrackerConfig jobTrackerConfig) {
        return staticConfig.addJobTrackerConfig(jobTrackerConfig);
    }

    @Override
    public Map<String, JobTrackerConfig> getJobTrackerConfigs() {
        return staticConfig.getJobTrackerConfigs();
    }

    @Override
    public Config setJobTrackerConfigs(Map<String, JobTrackerConfig> jobTrackerConfigs) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public Map<String, QuorumConfig> getQuorumConfigs() {
        return staticConfig.getQuorumConfigs();
    }

    @Override
    public QuorumConfig getQuorumConfig(String name) {
        return staticConfig.getQuorumConfig(name);
    }

    @Override
    public QuorumConfig findQuorumConfig(String name) {
        return staticConfig.findQuorumConfig(name);
    }

    @Override
    public Config setQuorumConfigs(Map<String, QuorumConfig> quorumConfigs) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public Config addQuorumConfig(QuorumConfig quorumConfig) {
        return staticConfig.addQuorumConfig(quorumConfig);
    }

    @Override
    public ManagementCenterConfig getManagementCenterConfig() {
        return staticConfig.getManagementCenterConfig();
    }

    @Override
    public Config setManagementCenterConfig(ManagementCenterConfig managementCenterConfig) {
        return staticConfig.setManagementCenterConfig(managementCenterConfig);
    }

    @Override
    public ServicesConfig getServicesConfig() {
        return staticConfig.getServicesConfig();
    }

    @Override
    public Config setServicesConfig(ServicesConfig servicesConfig) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public SecurityConfig getSecurityConfig() {
        return staticConfig.getSecurityConfig();
    }

    @Override
    public Config setSecurityConfig(SecurityConfig securityConfig) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public Config addListenerConfig(ListenerConfig listenerConfig) {
        return staticConfig.addListenerConfig(listenerConfig);
    }

    @Override
    public List<ListenerConfig> getListenerConfigs() {
        return staticConfig.getListenerConfigs();
    }

    @Override
    public Config setListenerConfigs(List<ListenerConfig> listenerConfigs) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public SerializationConfig getSerializationConfig() {
        return staticConfig.getSerializationConfig();
    }

    @Override
    public Config setSerializationConfig(SerializationConfig serializationConfig) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public PartitionGroupConfig getPartitionGroupConfig() {
        return staticConfig.getPartitionGroupConfig();
    }

    @Override
    public Config setPartitionGroupConfig(PartitionGroupConfig partitionGroupConfig) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public HotRestartPersistenceConfig getHotRestartPersistenceConfig() {
        return staticConfig.getHotRestartPersistenceConfig();
    }

    @Override
    public Config setHotRestartPersistenceConfig(HotRestartPersistenceConfig hrConfig) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public ManagedContext getManagedContext() {
        return staticConfig.getManagedContext();
    }

    @Override
    public Config setManagedContext(ManagedContext managedContext) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public ConcurrentMap<String, Object> getUserContext() {
        return staticConfig.getUserContext();
    }

    @Override
    public Config setUserContext(ConcurrentMap<String, Object> userContext) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public NativeMemoryConfig getNativeMemoryConfig() {
        return staticConfig.getNativeMemoryConfig();
    }

    @Override
    public Config setNativeMemoryConfig(NativeMemoryConfig nativeMemoryConfig) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public URL getConfigurationUrl() {
        return staticConfig.getConfigurationUrl();
    }

    @Override
    public Config setConfigurationUrl(URL configurationUrl) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public File getConfigurationFile() {
        return staticConfig.getConfigurationFile();
    }

    @Override
    public Config setConfigurationFile(File configurationFile) {
        return staticConfig.setConfigurationFile(configurationFile);
    }

    @Override
    public String getLicenseKey() {
        return staticConfig.getLicenseKey();
    }

    @Override
    public Config setLicenseKey(String licenseKey) {
        return staticConfig.setLicenseKey(licenseKey);
    }

    @Override
    public boolean isLiteMember() {
        return staticConfig.isLiteMember();
    }

    @Override
    public Config setLiteMember(boolean liteMember) {
        return staticConfig.setLiteMember(liteMember);
    }

    @Override
    public UserCodeDeploymentConfig getUserCodeDeploymentConfig() {
        return staticConfig.getUserCodeDeploymentConfig();
    }

    @Override
    public Config setUserCodeDeploymentConfig(UserCodeDeploymentConfig userCodeDeploymentConfig) {
        return staticConfig.setUserCodeDeploymentConfig(userCodeDeploymentConfig);
    }

    @Override
    public String toString() {
        return staticConfig.toString();
    }


    public void setConfigurationService(ClusterWideConfigurationService configurationService) {
        this.configurationService = configurationService;
    }
}
