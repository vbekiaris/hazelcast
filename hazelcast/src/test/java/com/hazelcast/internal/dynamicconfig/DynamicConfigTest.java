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

import com.hazelcast.config.CardinalityEstimatorConfig;
import com.hazelcast.config.DurableExecutorConfig;
import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.ItemListenerConfig;
import com.hazelcast.config.ListConfig;
import com.hazelcast.config.LockConfig;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.RingbufferStoreConfig;
import com.hazelcast.config.ScheduledExecutorConfig;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemListener;
import com.hazelcast.core.RingbufferStore;
import com.hazelcast.core.RingbufferStoreFactory;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static com.hazelcast.config.MultiMapConfig.ValueCollectionType.LIST;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class DynamicConfigTest extends HazelcastTestSupport {

    protected static final int INSTANCE_COUNT = 2;

    private String name = randomString();
    private TestHazelcastInstanceFactory factory;
    private HazelcastInstance[] members;
    // add***Config is invoked on driver instance
    private HazelcastInstance driver;

    @Before
    public void setup() {
        members = newInstances();
        driver = getDriver();
    }

    protected HazelcastInstance[] newInstances() {
        factory = createHazelcastInstanceFactory(INSTANCE_COUNT);
        HazelcastInstance[] instances = factory.newInstances();
        return instances;
    }

    protected HazelcastInstance getDriver() {
        return members[members.length - 1];
    }

    @Test
    public void testMultiMapConfig() {
        MultiMapConfig multiMapConfig = new MultiMapConfig(name);
        multiMapConfig.setBackupCount(4)
                      .setAsyncBackupCount(2)
                      .setStatisticsEnabled(true)
                      .setBinary(true)
                      .setValueCollectionType(LIST)
                      .addEntryListenerConfig(
                              new EntryListenerConfig("com.hazelcast.Listener", true, false)
                      );

        driver.getConfig().addMultiMapConfig(multiMapConfig);

        MultiMapConfig configOnCluster = getConfigurationService().getMultiMapConfig(name);
        assertEquals(multiMapConfig.getName(), configOnCluster.getName());
        assertEquals(multiMapConfig.getBackupCount(), configOnCluster.getBackupCount());
        assertEquals(multiMapConfig.getAsyncBackupCount(), configOnCluster.getAsyncBackupCount());
        assertEquals(multiMapConfig.isStatisticsEnabled(), configOnCluster.isStatisticsEnabled());
        assertEquals(multiMapConfig.isBinary(), configOnCluster.isBinary());
        assertEquals(multiMapConfig.getValueCollectionType(), configOnCluster.getValueCollectionType());
        assertEquals(multiMapConfig.getEntryListenerConfigs().get(0),
                configOnCluster.getEntryListenerConfigs().get(0));
    }

    @Ignore("TODO Currently fails because MapListenerToEntryListenerAdapter is not serializable")
    @Test
    public void testMultiMapConfig_whenEntryListenerConfigHasImplementation() {
        MultiMapConfig multiMapConfig = new MultiMapConfig(name);
        multiMapConfig.setBackupCount(4)
                      .setAsyncBackupCount(2)
                      .setStatisticsEnabled(true)
                      .setBinary(true)
                      .setValueCollectionType(LIST)
                      .addEntryListenerConfig(
                              new EntryListenerConfig(new SampleEntryListener(), true, false)
                      );

        driver.getConfig().addMultiMapConfig(multiMapConfig);

        MultiMapConfig configOnCluster = getConfigurationService().getMultiMapConfig(name);
        assertEquals(multiMapConfig.getName(), configOnCluster.getName());
        assertEquals(multiMapConfig.getBackupCount(), configOnCluster.getBackupCount());
        assertEquals(multiMapConfig.getAsyncBackupCount(), configOnCluster.getAsyncBackupCount());
        assertEquals(multiMapConfig.isStatisticsEnabled(), configOnCluster.isStatisticsEnabled());
        assertEquals(multiMapConfig.isBinary(), configOnCluster.isBinary());
        assertEquals(multiMapConfig.getValueCollectionType(), configOnCluster.getValueCollectionType());
        assertEquals(multiMapConfig.getEntryListenerConfigs().get(0),
                configOnCluster.getEntryListenerConfigs().get(0));
    }

    @Test
    public void testCardinalityEstimatorConfig() {
        CardinalityEstimatorConfig config = new CardinalityEstimatorConfig(name, 4 ,2);

        driver.getConfig().addCardinalityEstimatorConfig(config);

        CardinalityEstimatorConfig configOnCluster = getConfigurationService().getCardinalityEstimatorConfig(name);
        assertEquals(config.getName(), configOnCluster.getName());
        assertEquals(config.getBackupCount(), configOnCluster.getBackupCount());
        assertEquals(config.getAsyncBackupCount(), configOnCluster.getAsyncBackupCount());
    }

    @Test
    public void testLockConfig() {
        LockConfig config = new LockConfig(name);
        config.setQuorumName(randomString());

        driver.getConfig().addLockConfig(config);

        LockConfig configOnCluster = getConfigurationService().getLockConfig(name);
        assertEquals(config.getName(), configOnCluster.getName());
        assertEquals(config.getQuorumName(), configOnCluster.getQuorumName());
    }

    @Test
    public void testListConfig() {
        ListConfig config = getListConfig();

        driver.getConfig().addListConfig(config);

        ListConfig configOnCluster = getConfigurationService().getListConfig(name);
        assertListConfigCommons(config, configOnCluster);
    }

    @Test
    public void testListConfig_withItemListenerConfig_byClassName() {
        ListConfig config = getListConfig();
        List<ItemListenerConfig> itemListenerConfigs = new ArrayList<ItemListenerConfig>();
        ItemListenerConfig listenerConfig = new ItemListenerConfig("com.hazelcast.ItemListener", true);
        itemListenerConfigs.add(listenerConfig);
        config.setItemListenerConfigs(itemListenerConfigs);

        driver.getConfig().addListConfig(config);

        ListConfig configOnCluster = getConfigurationService().getListConfig(name);
        assertListConfigCommons(config, configOnCluster);
        ItemListenerConfig listenerConfigOnCluster = configOnCluster.getItemListenerConfigs().get(0);
        assertEquals(listenerConfig.getClassName(), listenerConfigOnCluster.getClassName());
        assertEquals(listenerConfig.isIncludeValue(), listenerConfigOnCluster.isIncludeValue());
    }

    @Test
    public void testListConfig_withItemListenerConfig_byImplementation() {
        ListConfig config = getListConfig();
        List<ItemListenerConfig> itemListenerConfigs = new ArrayList<ItemListenerConfig>();
        ItemListenerConfig listenerConfig = new ItemListenerConfig(new SampleItemListener(), true);
        itemListenerConfigs.add(listenerConfig);
        config.setItemListenerConfigs(itemListenerConfigs);

        driver.getConfig().addListConfig(config);

        ListConfig configOnCluster = getConfigurationService().getListConfig(name);
        assertListConfigCommons(config, configOnCluster);
        ItemListenerConfig listenerConfigOnCluster = configOnCluster.getItemListenerConfigs().get(0);
        assertEquals(listenerConfig.getImplementation(), listenerConfigOnCluster.getImplementation());
        assertEquals(listenerConfig.isIncludeValue(), listenerConfigOnCluster.isIncludeValue());
    }

    @Test
    public void testExecutorConfig() {
        ExecutorConfig config = new ExecutorConfig(name, 7);
        config.setStatisticsEnabled(true);
        config.setQueueCapacity(13);

        driver.getConfig().addExecutorConfig(config);

        ExecutorConfig configOnCluster = getConfigurationService().getExecutorConfig(name);
        assertEquals(config.getName(), configOnCluster.getName());
        assertEquals(config.getPoolSize(), configOnCluster.getPoolSize());
        assertEquals(config.getQueueCapacity(), configOnCluster.getQueueCapacity());
        assertEquals(config.isStatisticsEnabled(), configOnCluster.isStatisticsEnabled());
    }

    @Test
    public void testDurableExecutorConfig() {
        DurableExecutorConfig config = new DurableExecutorConfig(name, 7, 3, 10);

        driver.getConfig().addDurableExecutorConfig(config);

        DurableExecutorConfig configOnCluster = getConfigurationService().getDurableExecutorConfig(name);
        assertEquals(config.getName(), configOnCluster.getName());
        assertEquals(config.getPoolSize(), configOnCluster.getPoolSize());
        assertEquals(config.getCapacity(), configOnCluster.getCapacity());
        assertEquals(config.getDurability(), configOnCluster.getDurability());
    }

    @Test
    public void testScheduledExecutorConfig() {
        ScheduledExecutorConfig config = new ScheduledExecutorConfig(name, 2, 3, 10);

        driver.getConfig().addScheduledExecutorConfig(config);

        ScheduledExecutorConfig configOnCluster = getConfigurationService().getScheduledExecutorConfig(name);
        assertEquals(config.getName(), configOnCluster.getName());
        assertEquals(config.getPoolSize(), configOnCluster.getPoolSize());
        assertEquals(config.getCapacity(), configOnCluster.getCapacity());
        assertEquals(config.getDurability(), configOnCluster.getDurability());
    }

    @Test
    public void testRingbufferConfig() {
        RingbufferConfig config = getRingbufferConfig();

        driver.getConfig().addRingBufferConfig(config);

        RingbufferConfig configOnCluster = getConfigurationService().getRingbufferConfig(name);
        assertRingbufferConfigCommons(config, configOnCluster);
    }

    @Test
    public void testRingbufferConfig_whenConfiguredWithRingbufferStore_byClassName() {
        RingbufferConfig config = getRingbufferConfig();
        config.getRingbufferStoreConfig().setEnabled(true).setClassName("com.hazelcast.Foo");

        driver.getConfig().addRingBufferConfig(config);

        RingbufferConfig configOnCluster = getConfigurationService().getRingbufferConfig(name);
        assertRingbufferConfigCommons(config, configOnCluster);
        assertRingBufferStoreConfig(config.getRingbufferStoreConfig(), configOnCluster.getRingbufferStoreConfig());
    }

    @Test
    public void testRingbufferConfig_whenConfiguredWithRingbufferStore_byFactoryClassName() {
        RingbufferConfig config = getRingbufferConfig();
        config.getRingbufferStoreConfig().setEnabled(true).setFactoryClassName("com.hazelcast.FactoryFoo");

        driver.getConfig().addRingBufferConfig(config);

        RingbufferConfig configOnCluster = getConfigurationService().getRingbufferConfig(name);
        assertRingbufferConfigCommons(config, configOnCluster);
        assertRingBufferStoreConfig(config.getRingbufferStoreConfig(), configOnCluster.getRingbufferStoreConfig());
    }

    @Test
    public void testRingbufferConfig_whenConfiguredWithRingbufferStore_byStoreImplementation() {
        RingbufferConfig config = getRingbufferConfig();
        config.getRingbufferStoreConfig().setEnabled(true).setStoreImplementation(new SampleRingbufferStore());

        driver.getConfig().addRingBufferConfig(config);

        RingbufferConfig configOnCluster = getConfigurationService().getRingbufferConfig(name);
        assertRingbufferConfigCommons(config, configOnCluster);
        assertRingBufferStoreConfig(config.getRingbufferStoreConfig(), configOnCluster.getRingbufferStoreConfig());
    }

    @Test
    public void testRingbufferConfig_whenConfiguredWithRingbufferStore_byFactoryImplementation() {
        RingbufferConfig config = getRingbufferConfig();
        config.getRingbufferStoreConfig().setEnabled(true).setFactoryImplementation(new SampleRingbufferStoreFactory());

        driver.getConfig().addRingBufferConfig(config);

        RingbufferConfig configOnCluster = getConfigurationService().getRingbufferConfig(name);
        assertRingbufferConfigCommons(config, configOnCluster);
        assertRingBufferStoreConfig(config.getRingbufferStoreConfig(), configOnCluster.getRingbufferStoreConfig());
    }

    private ListConfig getListConfig() {
        ListConfig config = new ListConfig(name);
        config.setStatisticsEnabled(true)
              .setMaxSize(99)
              .setBackupCount(4)
              .setAsyncBackupCount(2);
        return config;
    }

    private void assertListConfigCommons(ListConfig config, ListConfig configOnCluster) {
        assertEquals(config.getName(), configOnCluster.getName());
        assertEquals(config.getBackupCount(), configOnCluster.getBackupCount());
        assertEquals(config.getAsyncBackupCount(), configOnCluster.getAsyncBackupCount());
        assertEquals(config.getMaxSize(), configOnCluster.getMaxSize());
        assertEquals(config.isStatisticsEnabled(), configOnCluster.isStatisticsEnabled());
    }

    private void assertRingBufferStoreConfig(RingbufferStoreConfig config, RingbufferStoreConfig configOnCluster) {
        assertEquals(config.getClassName(),
                configOnCluster.getClassName());
        assertEquals(config.getFactoryClassName(),
                configOnCluster.getFactoryClassName());
        assertEquals(config.getStoreImplementation(),
                configOnCluster.getStoreImplementation());
        assertEquals(config.getFactoryImplementation(),
                configOnCluster.getFactoryImplementation());
        assertEquals(config.isEnabled(),
                configOnCluster.isEnabled());
        assertPropertiesEqual(config.getProperties(),
                configOnCluster.getProperties());
    }

    private ClusterWideConfigurationService getConfigurationService() {
        return getNodeEngineImpl(members[members.length - 1]).getConfigurationService();
    }

    private RingbufferConfig getRingbufferConfig() {
        RingbufferConfig config = new RingbufferConfig(name);
        config.setTimeToLiveSeconds(59);
        config.setInMemoryFormat(InMemoryFormat.OBJECT);
        config.setCapacity(33);
        config.setBackupCount(4);
        config.setAsyncBackupCount(2);
        return config;
    }

    private void assertRingbufferConfigCommons(RingbufferConfig config, RingbufferConfig configOnCluster) {
        assertEquals(config.getName(), configOnCluster.getName());
        assertEquals(config.getAsyncBackupCount(), configOnCluster.getAsyncBackupCount());
        assertEquals(config.getBackupCount(), configOnCluster.getBackupCount());
        assertEquals(config.getCapacity(), configOnCluster.getCapacity());
        assertEquals(config.getInMemoryFormat(), configOnCluster.getInMemoryFormat());
        assertEquals(config.getTimeToLiveSeconds(), configOnCluster.getTimeToLiveSeconds());
    }

    private void assertPropertiesEqual(Properties expected, Properties actual) {
        if (expected == null) {
            assertNull(actual);
        }

        for (String key : expected.stringPropertyNames()) {
            assertEquals(expected.getProperty(key), actual.getProperty(key));
        }
    }

    public static class SampleEntryListener implements EntryAddedListener, Serializable {

        @Override
        public void entryAdded(EntryEvent event) {
        }
    }

    public static class SampleItemListener implements ItemListener, Serializable {

        @Override
        public void itemAdded(ItemEvent item) {
        }

        @Override
        public void itemRemoved(ItemEvent item) {
        }

        @Override
        public boolean equals(Object obj) {
            return (obj instanceof SampleItemListener);
        }
    }

    public static class SampleRingbufferStore implements RingbufferStore, Serializable {
        @Override
        public void store(long sequence, Object data) {
        }

        @Override
        public void storeAll(long firstItemSequence, Object[] items) {
        }

        @Override
        public Object load(long sequence) {
            return null;
        }

        @Override
        public long getLargestSequence() {
            return 0;
        }

        @Override
        public int hashCode() {
            return 33;
        }

        @Override
        public boolean equals(Object obj) {
            return (obj instanceof SampleRingbufferStore);
        }
    }

    public static class SampleRingbufferStoreFactory implements RingbufferStoreFactory, Serializable {
        @Override
        public RingbufferStore newRingbufferStore(String name, Properties properties) {
            return null;
        }

        @Override
        public boolean equals(Object obj) {
            return (obj instanceof SampleRingbufferStoreFactory);
        }
    }
}
