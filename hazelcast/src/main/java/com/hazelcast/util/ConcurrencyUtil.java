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

package com.hazelcast.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentMap;

/**
 * Utility methods to getOrPutSynchronized and getOrPutIfAbsent in a thread safe way
 * from ConcurrentMap with a ConstructorFunction.
 */
public final class ConcurrencyUtil {

    public static final Logger LOGGER = LoggerFactory.getLogger(ConcurrencyUtil.class);

    private ConcurrencyUtil() {
    }

    public static <K, V> V getOrPutSynchronized(ConcurrentMap<K, V> map, K key,
                                                final Object mutex, ConstructorFunction<K, V> func) {
        if (mutex == null) {
            throw new NullPointerException();
        }
        V value = map.get(key);
        if (value == null) {
            LOGGER.info("going to acquire mutex " + mutex.getClass()+"@"+System.identityHashCode(mutex));
            synchronized (mutex) {
                LOGGER.info("mutex acquired " + mutex.getClass()+"@"+System.identityHashCode(mutex));
                value = map.get(key);
                if (value == null) {
                    LOGGER.info("going to create new " + func + " via ctor func");
                    value = func.createNew(key);
                    LOGGER.info("created " + value + " -- of func " + func);
                    map.put(key, value);
                }
            }
        }
        return value;
    }

    public static <K, V> V getOrPutIfAbsent(ConcurrentMap<K, V> map, K key, ConstructorFunction<K, V> func) {
        V value = map.get(key);
        if (value == null) {
            value = func.createNew(key);
            V current = map.putIfAbsent(key, value);
            value = current == null ? value : current;
        }
        return value;
    }

}
