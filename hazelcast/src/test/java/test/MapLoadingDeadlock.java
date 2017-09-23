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

package test;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.fail;

/**
 * Test to ensure there is no deadlock when a new node joins during data loading.
 */
@Category(QuickTest.class)
public class MapLoadingDeadlock {

    private static final Logger logger = Logger.getLogger(MapLoadingDeadlock.class);

    private static final int preloadSize = 1000;

    private static final int writeDelaySeconds = 5;

    private static final AtomicInteger mapSize = new AtomicInteger(-1);

    private final String mapName = "map" + getClass().getSimpleName();

    private final InMemoryMapStoreSleepAndCount store = new InMemoryMapStoreSleepAndCount(1000);

    private final ExecutorService executorService = Executors.newFixedThreadPool(3);

    private final List<HazelcastInstance> hazelcastInstances = new ArrayList<HazelcastInstance>();

    @Before
    public void setUp() throws Exception {
        BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.ALL);
    }

    @After
    public void tearDown() {
        for (HazelcastInstance hazelcastInstance : hazelcastInstances) {
            hazelcastInstance.getLifecycleService().terminate();
        }
    }

    @Test
    public void testMapStoreLoad() throws InterruptedException {

        // load data in the store
        store.preload(preloadSize);

        // runnable that creates a new Hazelcast instance and checks the map size
        Runnable task = new Runnable() {
            @Override
            public void run() {
                HazelcastInstance hcInstance = Hazelcast.newHazelcastInstance(getConfig());
                hazelcastInstances.add(hcInstance);
                IMap<String, String> map = hcInstance.getMap(mapName);
                int size = map.size();
                mapSize.set(size);
            }
        };

        // start first Hazelcast instance
        executorService.submit(task);

        // wait 6s so that the first instance triggers data loading on the map
        Thread.sleep(6000);

        // start second Hazelcast instance
        executorService.submit(task);

        // wait at most 150s
        final long t0 = System.currentTimeMillis();
        while ((System.currentTimeMillis() - t0) < 150000) {
            if (mapSize.get() == preloadSize) {
                break;
            }
            Thread.sleep(1000);
            logger.info(String.format("Loaded keys: %s", store.getCountLoadedKeys()));
        }

        // check for errors
        if (mapSize.get() != preloadSize) {
            fail(String.format("Not all data loaded (%s != %s).", mapSize.get(), preloadSize));
        }
    }

    private Config getConfig() {

        // create shared hazelcast config
        final Config config = new XmlConfigBuilder().build();
        config.setProperty("hazelcast.logging.type", "log4j");
        config.setProperty("hazelcast.diagnostics.enabled", "true");
//        config.setProperty("hazelcast.initial.min.cluster.size", "2");

        // disable JMX to make sure lazy loading works asynchronously
        config.setProperty("hazelcast.jmx", "false");

        // get map config
        MapConfig mapConfig = config.getMapConfig(mapName);

        // configure map store
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setInitialLoadMode(MapStoreConfig.InitialLoadMode.EAGER);
        mapStoreConfig.setWriteDelaySeconds(writeDelaySeconds);
        mapStoreConfig.setClassName(null);
        InMemoryMapStoreSleepAndCount testStore = new InMemoryMapStoreSleepAndCount(1000);
        testStore.preload(preloadSize);
        mapStoreConfig.setImplementation(testStore);
        mapConfig.setMapStoreConfig(mapStoreConfig);

        return config;
    }

}
