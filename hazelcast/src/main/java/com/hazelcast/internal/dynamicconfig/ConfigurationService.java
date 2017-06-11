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
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.util.Map;

public interface ConfigurationService {
    MultiMapConfig getMultiMapConfig(String name);

    MapConfig getMapConfig(String name);

    Map<String, MapConfig> getMapConfigs();

    TopicConfig getTopicConfig(String name);

    CardinalityEstimatorConfig getCardinalityEstimatorConfig(String name);

    ExecutorConfig getExecutorConfig(String name);

    ScheduledExecutorConfig getScheduledExecutorConfig(String name);

    DurableExecutorConfig getDurableExecutorConfig(String name);

    SemaphoreConfig getSemaphoreConfig(String name);

    RingbufferConfig getRingbufferConfig(String name);

    LockConfig getLockConfig(String name);

    Map<String, LockConfig> getLockConfigs();

    ListConfig getListConfig(String name);

    void registerLocally(IdentifiedDataSerializable config);

    void broadcastConfig(IdentifiedDataSerializable config);

    QueueConfig getQueueConfig(String name);

    Map<String, QueueConfig> getQueueConfigs();

    Map<String, ListConfig> getListConfigs();

    SetConfig getSetConfig(String name);

    Map<String, SetConfig> getSetConfigs();

    Map<String, MultiMapConfig> getMultiMapConfigs();

    ReplicatedMapConfig getReplicatedMapConfig(String name);

    Map<String, ReplicatedMapConfig> getReplicatedMapConfigs();

    Map<String, RingbufferConfig> getRingbufferConfigs();

    Map<String, TopicConfig> getTopicConfigs();

    ReliableTopicConfig getReliableTopicConfig(String name);

    Map<String, ReliableTopicConfig> getReliableTopicConfigs();

    Map<String, ExecutorConfig> getExecutorConfigs();

    Map<String, DurableExecutorConfig> getDurableExecutorConfigs();

    Map<String, ScheduledExecutorConfig> getScheduledExecutorConfigs();

    Map<String, CardinalityEstimatorConfig> getCardinalityEstimatorConfigs();

    Map<String, SemaphoreConfig> getSemaphoreConfigs();

    CacheSimpleConfig getCacheConfig(String name);

    Map<String,CacheSimpleConfig> getCacheSimpleConfigs();
}

