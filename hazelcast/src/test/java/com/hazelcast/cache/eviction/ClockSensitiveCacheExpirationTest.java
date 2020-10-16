/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache.eviction;

import com.hazelcast.cache.CacheTestSupport;
import com.hazelcast.cache.HazelcastExpiryPolicy;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.backup.BackupAccessor;
import com.hazelcast.test.backup.TestBackupUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import javax.cache.Cache;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.configuration.MutableCacheEntryListenerConfiguration;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryExpiredListener;
import javax.cache.event.CacheEntryListener;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.internal.util.ClockProperties.HAZELCAST_CLOCK_IMPL;
import static com.hazelcast.test.backup.TestBackupUtils.assertBackupSizeEventually;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(PowerMockRunner.class)
@PrepareForTest(Clock.class)
@Category({QuickTest.class, ParallelJVMTest.class})
@PowerMockIgnore({"javax.net.ssl.*", "javax.security.*", "javax.management.*"})
public class ClockSensitiveCacheExpirationTest extends CacheTestSupport {

    @Rule
    OverridePropertyRule overrideProperty = OverridePropertyRule.set(HAZELCAST_CLOCK_IMPL,
            "com.hazelcast.cache.eviction.ClockSensitiveCacheExpirationTest$ManipulatedClock");

    private boolean useSyncBackups;

    private static final int KEY_RANGE = 1000;
    private static final int CLUSTER_SIZE = 2;

    private TestHazelcastInstanceFactory factory;
    private HazelcastInstance[] instances = new HazelcastInstance[3];

    @Override
    protected HazelcastInstance getHazelcastInstance() {
        return instances[0];
    }

    @Override
    protected void onSetup() {
        factory = createHazelcastInstanceFactory(CLUSTER_SIZE);
        for (int i = 0; i < CLUSTER_SIZE; i++) {
            instances[i] = factory.newHazelcastInstance(getConfig());
        }
    }

    @Override
    protected Config getConfig() {
        return smallInstanceConfig();
    }

    @Override
    protected void onTearDown() {
        factory.shutdownAll();
    }

    @Test
    public void test_whenEntryIsRemoved_syncBackupIsCleaned() {
        useSyncBackups = true;
        test_whenEntryIsRemovedBackupIsCleaned();
    }

    @Test
    public void test_whenEntryIsRemoved_asyncBackupIsCleaned() {
        useSyncBackups = false;
        test_whenEntryIsRemovedBackupIsCleaned();
    }

    public void test_whenEntryIsRemovedBackupIsCleaned() {
        SimpleExpiryListener listener = new SimpleExpiryListener();
        int ttlSeconds = 3;
        Duration duration = new Duration(TimeUnit.SECONDS, ttlSeconds);
        HazelcastExpiryPolicy expiryPolicy = new HazelcastExpiryPolicy(duration, duration, duration);
        CacheConfig<Integer, Integer> cacheConfig = createCacheConfig(expiryPolicy, listener);
        Cache<Integer, Integer> cache = createCache(cacheConfig);

        ManipulatedClock.stop();
        for (int i = 0; i < KEY_RANGE; i++) {
            cache.put(i, i);
            assertTrue("Expected to remove entry " + i + " but entry was not present. Expired entry count: "
                    + listener.getExpirationCount().get(), cache.remove(i));
        }
        // advance time 4 seconds
        ManipulatedClock.advanceMillis(4000);

        assertEquals(0, listener.getExpirationCount().get());
        for (int i = 1; i < CLUSTER_SIZE; i++) {
            BackupAccessor backupAccessor = TestBackupUtils.newCacheAccessor(instances, cache.getName(), i);
            assertBackupSizeEventually(0, backupAccessor);
        }
    }

    protected <K, V, M extends Serializable & ExpiryPolicy, T extends Serializable & CacheEntryListener<K, V>>
    CacheConfig<K, V> createCacheConfig(M expiryPolicy, T listener) {
        CacheConfig<K, V> cacheConfig = createCacheConfig(expiryPolicy);
        MutableCacheEntryListenerConfiguration<K, V> listenerConfiguration = new MutableCacheEntryListenerConfiguration<K, V>(
                FactoryBuilder.factoryOf(listener), null, true, true
        );
        cacheConfig.addCacheEntryListenerConfiguration(listenerConfiguration);
        return cacheConfig;
    }

    protected <K, V, M extends Serializable & ExpiryPolicy> CacheConfig<K, V> createCacheConfig(M expiryPolicy) {
        CacheConfig<K, V> cacheConfig = new CacheConfig<>();
        cacheConfig.setExpiryPolicyFactory(FactoryBuilder.factoryOf(expiryPolicy));
        cacheConfig.setName(randomName());

        if (useSyncBackups) {
            cacheConfig.setBackupCount(CLUSTER_SIZE - 1);
            cacheConfig.setAsyncBackupCount(0);
        } else {
            cacheConfig.setBackupCount(0);
            cacheConfig.setAsyncBackupCount(CLUSTER_SIZE - 1);
        }

        return cacheConfig;
    }

    public static final class ManipulatedClock
            extends com.hazelcast.internal.util.Clock.ClockImpl {

        static final long NOT_SET = -1;

        static volatile long overriddenValue = NOT_SET;

        @Override
        protected long currentTimeMillis() {
            if (overriddenValue == NOT_SET) {
                return System.currentTimeMillis();
            } else {
                return overriddenValue;
            }
        }

        static synchronized void stop() {
            overriddenValue = System.currentTimeMillis();
        }

        static synchronized void advanceMillis(long millis) {
            overriddenValue += millis;
        }

        static synchronized void start() {
            overriddenValue = NOT_SET;
        }
    }

    public static class SimpleExpiryListener<K, V> implements CacheEntryExpiredListener<K, V>, Serializable {

        private AtomicInteger expirationCount = new AtomicInteger();

        @Override
        public void onExpired(Iterable<CacheEntryEvent<? extends K, ? extends V>> cacheEntryEvents) throws
                CacheEntryListenerException {
            expirationCount.incrementAndGet();
        }

        public AtomicInteger getExpirationCount() {
            return expirationCount;
        }
    }
}
