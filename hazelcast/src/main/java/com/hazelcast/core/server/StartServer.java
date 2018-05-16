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

package com.hazelcast.core.server;

import com.hazelcast.cache.HazelcastCachingProvider;
import com.hazelcast.cache.ICache;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.spi.CachingProvider;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;

import static com.hazelcast.nio.IOUtil.closeResource;

/**
 * Starts a Hazelcast server.
 */
public final class StartServer {

    private StartServer() {
    }

    /**
     * Creates a server instance of Hazelcast.
     * <p>
     * If user sets the system property "print.port", the server writes the port number of the Hazelcast instance to a file.
     * The file name is the same as the "print.port" property.
     *
     * @param args none
     */
//    public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException {
//        Config config = new Config();
//        config.getCacheConfig("test")
//              .setBackupCount(6);
//        HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);
//        CachingProvider provider = Caching.getCachingProvider();
//        CacheManager cacheManager = provider.getCacheManager(null, null, HazelcastCachingProvider.propertiesByInstanceItself(hz));
//        Cache<Integer, Integer> cache = cacheManager.createCache("test", new CacheConfig<Integer, Integer>()
//                        .setBackupCount(3));
//
//        ICache iCache = cache.unwrap(ICache.class);
//        System.out.println(((CacheConfig) iCache.getConfiguration(CacheConfig.class)).getBackupCount());
//        printMemberPort(hz);
//    }

    public static void main(String[] args) {

        MapConfig mapConfig = new MapConfig("test").setBackupCount(6);
        Config config = new Config();
        config.addMapConfig(mapConfig);

        HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);
        hz.getConfig().addMapConfig(mapConfig);

        System.out.println(">>> " + hz.getConfig().getMapConfig("test").getBackupCount());
    }

    private static void printMemberPort(HazelcastInstance hz) throws FileNotFoundException, UnsupportedEncodingException {
        String printPort = System.getProperty("print.port");
        if (printPort != null) {
            PrintWriter printWriter = null;
            try {
                printWriter = new PrintWriter("ports" + File.separator + printPort, "UTF-8");
                printWriter.println(hz.getCluster().getLocalMember().getAddress().getPort());
            } finally {
                closeResource(printWriter);
            }
        }
    }
}
