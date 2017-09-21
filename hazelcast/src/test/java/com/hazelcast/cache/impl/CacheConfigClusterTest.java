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

package com.hazelcast.cache.impl;

import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.FilteringClassLoader;
import com.hazelcast.util.FutureUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class CacheConfigClusterTest extends HazelcastTestSupport {

    @Test
    public void testMemberCanJoinCluster_whenCacheKVTypesUnknown() {
        FilteringClassLoader classLoader = new FilteringClassLoader(
                asList("classloading.DomainClass"), null);

        CacheSimpleConfig typedCacheConfig = new CacheSimpleConfig()
                .setName("typed-cache")
                .setKeyType("java.lang.Integer")
                .setValueType("classloading.DomainClass");
        Config config = new Config().addCacheConfig(typedCacheConfig);
        final Config missingClassesConfig = new Config().setClassLoader(classLoader);
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance memberWithClasses = factory.newHazelcastInstance(config);
        Cache typedCache = memberWithClasses.getCacheManager().getCache("typed-cache");
        final AtomicReference<HazelcastInstance> memberMissingClassesRef = new AtomicReference<HazelcastInstance>();
        Collection<Future> memberStartupFuture = Arrays.asList(spawn(new Runnable() {
            @Override
            public void run() {
                memberMissingClassesRef.set(factory.newHazelcastInstance(missingClassesConfig));
            }
        }));
        FutureUtil.waitWithDeadline(memberStartupFuture, 30, TimeUnit.SECONDS, FutureUtil.RETHROW_EVERYTHING);

        final HazelcastInstance memberMissingClasses = memberMissingClassesRef.get();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertEquals(2, memberWithClasses.getCluster().getMembers().size());
                assertEquals(2, memberMissingClasses.getCluster().getMembers().size());
            }
        }, 15);
        factory.terminateAll();
    }

    public static class CacheValue implements Serializable {

    }
}
