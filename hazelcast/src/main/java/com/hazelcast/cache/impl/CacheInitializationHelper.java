/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.util.EmptyStatement;

/**
 *
 */
public final class CacheInitializationHelper {

    public static final Class ICACHE_SERVICE_CLASS;

    public static final String CACHE_SERVICE_CLASS_NAME = "com.hazelcast.cache.impl.CacheService";
    public static final String ICACHE_SERVICE_CLASS_NAME = "com.hazelcast.cache.ICacheService";

    public static final String SERVICE_NAME = "hz:impl:cacheService";

    public static final String DEFAULT_CACHE_MERGE_POLICY = "com.hazelcast.cache.merge.PassThroughCacheMergePolicy";

    static {
        Class iCacheServiceClass = null;
        try {
            iCacheServiceClass = ClassLoaderUtil.tryLoadClass("com.hazelcast.cache.impl.ICacheService");
        } catch (ClassNotFoundException e) {
            // hazelcast-jcache is not available in classpath
            EmptyStatement.ignore(e);
        }
        ICACHE_SERVICE_CLASS = iCacheServiceClass;
    }

    private CacheInitializationHelper() {

    }
}
