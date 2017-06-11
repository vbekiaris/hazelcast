package com.hazelcast.internal.dynamicconfig;

import com.hazelcast.config.Config;
import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.config.TopicConfig;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.MapEvent;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.hazelcast.core.MultiMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class DynamicConfigSmokeTest extends HazelcastTestSupport {

    @After
    public void classCleanup() {
        DummyEntryListener.addedCounter.set(0);
        DummyMessageListener.messageCounter.set(0);
    }

    @Test
    public void multimap_smokeMultimap_initialTest() {
        String mapName = "dynamicMM";
        final int initialClusterSize = 3;

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(initialClusterSize);
        HazelcastInstance i1 = factory.newInstances()[0];

        MultiMapConfig dynamicMultimapConfig = new MultiMapConfig(mapName);
        dynamicMultimapConfig.addEntryListenerConfig(new EntryListenerConfig(DummyEntryListener.class.getName(), false, false));
        Config dynamicConfig1 = i1.getConfig();
        dynamicConfig1.addMultiMapConfig(dynamicMultimapConfig);

        MultiMap<String, String> multiMap = i1.getMultiMap(mapName);
        multiMap.put("foo", "1");

        assertEqualsEventually(initialClusterSize, DummyEntryListener.addedCounter);
    }

    @Test
    public void multimap_withNewMemberJoiningLater() {
        String mapName = "dynamicMM";

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance i1 = factory.newHazelcastInstance();
        HazelcastInstance i2 = factory.newHazelcastInstance();

        MultiMapConfig dynamicMultimapConfig = new MultiMapConfig(mapName);
        dynamicMultimapConfig.addEntryListenerConfig(new EntryListenerConfig(DummyEntryListener.class.getName(), false, false));
        Config dynamicConfig1 = i1.getConfig();
        dynamicConfig1.addMultiMapConfig(dynamicMultimapConfig);

        //start an instance AFTER the config was already submitted
        factory.newHazelcastInstance();

        MultiMap<String, String> multiMap = i1.getMultiMap(mapName);
        multiMap.put("foo", "1");


        assertEqualsEventually(3, DummyEntryListener.addedCounter);
    }

    @Test
    public void map_withNewMemberJoiningLater() {
        String mapName = "dynamicMam";

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance i1 = factory.newHazelcastInstance();
        HazelcastInstance i2 = factory.newHazelcastInstance();

        MapConfig dynamicMultimapConfig = new MapConfig(mapName);
        dynamicMultimapConfig.addEntryListenerConfig(new EntryListenerConfig(DummyEntryListener.class.getName(), false, false));
        Config dynamicConfig1 = i1.getConfig();
        dynamicConfig1.addMapConfig(dynamicMultimapConfig);

        //start an instance AFTER the config was already submitted
        factory.newHazelcastInstance();

        IMap<String, String> map = i1.getMap(mapName);
        map.put("foo", "1");

        assertEqualsEventually(3, DummyEntryListener.addedCounter);
    }

    @Test
    public void topic_initialTest() {
        String topicName = "dynamicTopic";
        String mapName = "dynamicMM";
        final int initialClusterSize = 3;

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(initialClusterSize);
        HazelcastInstance i1 = factory.newInstances()[0];

        TopicConfig topicConfig = new TopicConfig(topicName);
        topicConfig.addMessageListenerConfig(new ListenerConfig(DummyMessageListener.class.getName()));
        i1.getConfig().addTopicConfig(topicConfig);

        ITopic<String> topic = i1.getTopic(topicName);
        topic.publish("foo");

        assertEqualsEventually(initialClusterSize, DummyMessageListener.messageCounter);
    }

    public static class DummyMessageListener implements MessageListener<String> {
        public static final AtomicInteger messageCounter = new AtomicInteger();

        @Override
        public void onMessage(Message<String> message) {
            messageCounter.incrementAndGet();
        }
    }

    public static class DummyEntryListener implements EntryListener, Serializable {
        public static final AtomicInteger addedCounter = new AtomicInteger();

        @Override
        public void entryAdded(EntryEvent event) {
            addedCounter.incrementAndGet();
        }

        @Override
        public void entryUpdated(EntryEvent event) {

        }

        @Override
        public void entryRemoved(EntryEvent event) {

        }

        @Override
        public void mapCleared(MapEvent event) {

        }

        @Override
        public void mapEvicted(MapEvent event) {

        }

        @Override
        public void entryEvicted(EntryEvent event) {

        }
    }
}
