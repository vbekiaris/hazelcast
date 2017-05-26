package com.hazelcast.internal.dynamicconfig;

import com.hazelcast.config.Config;
import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapEvent;
import com.hazelcast.core.MultiMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.atomic.AtomicInteger;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class DynamicConfigSmokeTest extends HazelcastTestSupport {

    @AfterClass
    public static void classCleanup() {
        DummyListener.addedCounter.set(0);
    }

    @Test
    public void multimap_smokeMultimap_initialTest() {
        String mapName = "dynamicMM";
        final int initialClusterSize = 3;

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(initialClusterSize);
        HazelcastInstance i1 = factory.newInstances()[0];

        MultiMapConfig dynamicMultimapConfig = new MultiMapConfig(mapName);
        dynamicMultimapConfig.addEntryListenerConfig(new EntryListenerConfig(DummyListener.class.getName(), false, false));
        Config dynamicConfig1 = i1.getConfig();
        dynamicConfig1.addMultiMapConfig(dynamicMultimapConfig);

        MultiMap<String, String> multiMap = i1.getMultiMap(mapName);
        multiMap.put("foo", "1");

        assertEqualsEventually(initialClusterSize, DummyListener.addedCounter);
    }

    @Test
    public void multimap_withNewMemberJoiningLater() {
        String mapName = "dynamicMM";

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance i1 = factory.newHazelcastInstance();
        HazelcastInstance i2 = factory.newHazelcastInstance();

        MultiMapConfig dynamicMultimapConfig = new MultiMapConfig(mapName);
        dynamicMultimapConfig.addEntryListenerConfig(new EntryListenerConfig(DummyListener.class.getName(), false, false));
        Config dynamicConfig1 = i1.getConfig();
        dynamicConfig1.addMultiMapConfig(dynamicMultimapConfig);

        //start an instance AFTER the config was already submitted
        factory.newHazelcastInstance();

        MultiMap<String, String> multiMap = i1.getMultiMap(mapName);
        multiMap.put("foo", "1");


        assertEqualsEventually(3, DummyListener.addedCounter);
    }

    @Test
    public void map_withNewMemberJoiningLater() {
        String mapName = "dynamicMam";

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance i1 = factory.newHazelcastInstance();
        HazelcastInstance i2 = factory.newHazelcastInstance();

        MapConfig dynamicMultimapConfig = new MapConfig(mapName);
        dynamicMultimapConfig.addEntryListenerConfig(new EntryListenerConfig(DummyListener.class.getName(), false, false));
        Config dynamicConfig1 = i1.getConfig();
        dynamicConfig1.addMapConfig(dynamicMultimapConfig);

        //start an instance AFTER the config was already submitted
        factory.newHazelcastInstance();

        IMap<String, String> map = i1.getMap(mapName);
        map.put("foo", "1");

        assertEqualsEventually(3, DummyListener.addedCounter);
    }

    public static class DummyListener implements EntryListener {
        private static final AtomicInteger addedCounter = new AtomicInteger();

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
