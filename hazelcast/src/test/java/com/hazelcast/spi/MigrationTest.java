package com.hazelcast.spi;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapLoaderLifecycleSupport;
import com.hazelcast.core.MapStore;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.Properties;

@Category(QuickTest.class)
public class MigrationTest {

    public static final Logger logger = LoggerFactory.getLogger(MigrationTest.class);

    @Test
    //@Ignore
    public void testDataMigration() throws Exception {

        Config shConfig = new Config();
        shConfig.getNetworkConfig().setPortAutoIncrement(true);
        shConfig.getGroupConfig().setName("schema");
        shConfig.getGroupConfig().setPassword("schemapass");
        JoinConfig shJoin = shConfig.getNetworkConfig().getJoin();
        shJoin.getMulticastConfig().setEnabled(false);
        shJoin.getTcpIpConfig().addMember("localhost").setEnabled(true);
        shConfig.setProperty("hazelcast.logging.type", "slf4j");
        shConfig.setLiteMember(false);
        shConfig.getServicesConfig().setEnableDefaults(true);

        MapConfig mConfig = new MapConfig("test");
        mConfig.getMapStoreConfig().setEnabled(true);
        mConfig.getMapStoreConfig().setWriteDelaySeconds(10);
        mConfig.getMapStoreConfig().setImplementation(new TestCacheStore());
        shConfig.addMapConfig(mConfig);

        HazelcastInstance node1 = Hazelcast.newHazelcastInstance(shConfig);
        populateCache(node1, "test", 1000);

        logger.info("population finished, time to start second node!");
        Thread.sleep(240000);
//
//        HazelcastInstance node2 = Hazelcast.newHazelcastInstance(shConfig);
//        logger.info("Starting test on node2");
//        populateCache(node2, "test", 1000);
//
//        System.out.println("Second node started");
//        Thread.sleep(60000);
    }

    private void populateCache(HazelcastInstance node, String cName, int count) {

        IMap<Long, String> cache = node.getMap(cName);
        int idx = cache.size();
        for (long i=idx; i < idx+count; i++) {
            cache.set(i, String.valueOf(i));
        }
    }


    public class TestCacheStore implements MapStore<Integer, String>, MapLoaderLifecycleSupport {

        private IMap testCache;

        @Override
        public String load(Integer key) {
            logger.error("received load request for key " + key);
            return null;
        }

        @Override
        public Map<Integer, String> loadAll(Collection<Integer> keys) {
            logger.error("received loadAll request for keys " + keys);
            return null;
        }

        @Override
        public Iterable<Integer> loadAllKeys() {
            logger.error("received loadAllKeys request");
            return null; //Collections.emptySet();
        }

        @Override
        public void init(HazelcastInstance hzInstance, Properties properties, String mapName) {
            logger.error("received init request for map " + mapName);
            testCache = hzInstance.getMap("test2");
        }

        @Override
        public void destroy() {
            // TODO Auto-generated method stub
            logger.error("destroy invoked");
        }

        @Override
        public void store(Integer key, String value) {
            // TODO Auto-generated method stub
            logger.error("store");
        }

        @Override
        public void storeAll(Map<Integer, String> map) {
            // TODO Auto-generated method stub
            logger.error("storeAll");
        }

        @Override
        public void delete(Integer key) {
            // TODO Auto-generated method stub
            logger.error("delete {}", key);
        }

        @Override
        public void deleteAll(Collection<Integer> keys) {
            // TODO Auto-generated method stub
            logger.error("delete all");
        }

    }
}