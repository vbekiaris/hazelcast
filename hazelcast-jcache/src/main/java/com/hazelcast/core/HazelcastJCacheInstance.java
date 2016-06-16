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

package com.hazelcast.core;

import com.hazelcast.cache.ICache;

/**
 *
 */
public interface HazelcastJCacheInstance implements HazelcastInstance {

    /**
     * <p>
     * Returns the cache instance with the specified prefixed cache name.
     * </p>
     *
     * <p>
     * Prefixed cache name is the name with URI and classloader prefixes if available.
     * There is no Hazelcast prefix (`/hz/`). For example, `myURI/foo`.
     *
     * <code>
     *     <prefixed_cache_name> = [<uri_prefix>/] + [<cl_prefix>/] + <pure_cache_name>
     * </code>
     * where `<pure_cache_name>` is the cache name without any prefix. For example `foo`.
     *
     * As seen from the definition, URI and classloader prefixes are optional.
     *
     * URI prefix is generated as content of this URI as a US-ASCII string. (<code>uri.toASCIIString()</code>)
     * Classloader prefix is generated as string representation of the specified classloader. (<code>cl.toString()</code>)
     *
     * @see com.hazelcast.cache.CacheUtil#getPrefixedCacheName(String, java.net.URI, ClassLoader)
     * </p>
     *
     * @param name the prefixed name of the cache
     * @return the cache instance with the specified prefixed name
     *
     * @throws com.hazelcast.cache.CacheNotExistsException  if there is no configured or created cache
     *                                                      with the specified prefixed name
     * @throws java.lang.IllegalStateException              if a valid (rather than `1.0.0-PFD` or `0.x` versions)
     *                                                      JCache library is not exist at classpath
     */
    <K, V> ICache<K, V> getCache(String name);
}
