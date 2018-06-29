/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spring.cache;

import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.cache.jcache.JCacheCacheManager;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.annotation.Resource;
import javax.cache.CacheManager;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"cacheManager-perInstance.xml"})
@Category(QuickTest.class)
public class JCacheCacheManagerPerInstanceTest {

    @Resource(name = "cacheManager")
    private JCacheCacheManager springCacheManager;

    @Resource(name = "cacheManager1")
    private CacheManager cacheManager1;

    @Resource(name = "cacheManager2")
    private CacheManager cacheManager2;

    @Resource(name = "cacheManager3")
    private CacheManager cacheManager3;

    @Test
    public void testCacheManagerIdentity() {
        assertSame(cacheManager1, cacheManager2);
        assertNotEquals(cacheManager1, cacheManager3);
    }

}
