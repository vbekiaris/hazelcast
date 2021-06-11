/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.Config;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.instance.impl.OutOfMemoryErrorDispatcher;

import javax.annotation.Nonnull;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Factory for {@link HazelcastInstance}'s, a node in a cluster.
 */
public final class Hazelcast {

    /** checkstyle comment */
    public static final ConcurrentHashMap<Long, ConcurrentSkipListSet<DasKey>> REMOVED_KEYS_BY_PREFIX
            = new ConcurrentHashMap<>();

    private Hazelcast() {
    }

    public static void addRemovedKey(long prefix, long address, long seq) {
        Exception e = null;
        if (seq == 0) {
            e = new HazelcastException("seq was 0");
        }
        REMOVED_KEYS_BY_PREFIX.computeIfAbsent(prefix, k -> new ConcurrentSkipListSet<>())
                       .add(new DasKey(prefix, address, seq, e));
    }

    public static void clearRemovedKeys(long prefix) {
        REMOVED_KEYS_BY_PREFIX.put(prefix, new ConcurrentSkipListSet<>());
    }

    public static void dumpRemovalsOf(long address) {
        System.out.println("Lookups of " + address);
        for (ConcurrentSkipListSet<DasKey> keys : REMOVED_KEYS_BY_PREFIX.values()) {
            for (DasKey key : keys) {
                if (key.address == address) {
                    System.out.println("\twas removed from prefix " + key.prefix + " with seq " + key.seq);
                    if (key.exception != null) {
                        StringWriter sw = new StringWriter();
                        key.exception.printStackTrace(new PrintWriter(sw));
                        String exceptionAsString = sw.toString();
                        System.out.println("\t>>> " + exceptionAsString);
                    }
                }
            }
        }
    }

    private static final class DasKey implements Comparable<DasKey> {
        final long prefix;
        final long address;
        final long seq;
        final Exception exception;

        DasKey(long prefix, long address, long seq) {
            this.prefix = prefix;
            this.address = address;
            this.seq = seq;
            this.exception = null;
        }

        DasKey(long prefix, long address, long seq, Exception exception) {
            this.prefix = prefix;
            this.address = address;
            this.seq = seq;
            this.exception = exception;
        }

        @Override
        public String toString() {
            if (exception != null) {
                String s = "Exceptional DasKey{" + "prefix=" + prefix + ", address=" + address + ", seq=" + seq + ", exception="
                        + exception + '}';
                System.out.println(">>> " + s);
                exception.printStackTrace();
                System.out.println(">>>");
                return s;
            }
            return "DasKey{" + "prefix=" + prefix + ", address=" + address + ", seq=" + seq + ", exception=" + exception + '}';
        }

        @Override
        public int compareTo(@Nonnull DasKey o) {
            if (prefix < o.prefix) {
                return -1;
            } else if (prefix > o.prefix) {
                return 1;
            } else {
                if (address < o.address) {
                    return -1;
                } else if (address > o.address) {
                    return 1;
                } else {
                    if (seq < o.seq) {
                        return -1;
                    } else if (seq > o.seq) {
                        return 1;
                    } else {
                        return 0;
                    }
                }
            }
        }
    }

    /**
     * Shuts down all member {@link HazelcastInstance}s running on this JVM.
     * It doesn't shutdown all members of the cluster but just the ones running on this JVM.
     *
     * @see #newHazelcastInstance(Config)
     */
    public static void shutdownAll() {
        HazelcastInstanceFactory.shutdownAll();
    }

    /**
     * Creates a new HazelcastInstance (a new node in a cluster).
     * This method allows you to create and run multiple instances
     * of Hazelcast cluster members on the same JVM.
     * <p>
     * To shutdown all running HazelcastInstances (all members on this JVM)
     * call {@link #shutdownAll()}.
     *
     * @param config Configuration for the new HazelcastInstance (member)
     * @return the new HazelcastInstance
     * @see #shutdownAll()
     * @see #getHazelcastInstanceByName(String)
     */
    public static HazelcastInstance newHazelcastInstance(Config config) {
        return HazelcastInstanceFactory.newHazelcastInstance(config);
    }

    /**
     * Creates a new HazelcastInstance (a new node in a cluster).
     * This method allows you to create and run multiple instances
     * of Hazelcast cluster members on the same JVM.
     * <p>
     * To shutdown all running HazelcastInstances (all members on this JVM)
     * call {@link #shutdownAll()}.
     *
     * Hazelcast will look into two places for the configuration file:
     * <ol>
     *     <li>
     *         System property: Hazelcast will first check if "hazelcast.config" system property is set to a file or a
     *         {@code classpath:...} path.
     *         Examples: -Dhazelcast.config=C:/myhazelcast.xml , -Dhazelcast.config=classpath:the-hazelcast-config.xml ,
     *         -Dhazelcast.config=classpath:com/mydomain/hazelcast.xml
     *     </li>
     *     <li>
     *         "hazelcast.xml" file in current working directory
     *     </li>
     *     <li>
     *         Classpath: Hazelcast will check classpath for hazelcast.xml file.
     *     </li>
     * </ol>
     * If Hazelcast doesn't find any config file, it will start with the default configuration (hazelcast-default.xml)
     * located in hazelcast.jar.
     *
     * @return the new HazelcastInstance
     * @see #shutdownAll()
     * @see #getHazelcastInstanceByName(String)
     */
    public static HazelcastInstance newHazelcastInstance() {
        return HazelcastInstanceFactory.newHazelcastInstance(null);
    }

    /**
     * Returns an existing HazelcastInstance with instanceName.
     * <p>
     * To shutdown all running HazelcastInstances (all members on this JVM)
     * call {@link #shutdownAll()}.
     *
     * @param instanceName Name of the HazelcastInstance (member)
     * @return an existing HazelcastInstance
     * @see #newHazelcastInstance(Config)
     * @see #shutdownAll()
     */
    public static HazelcastInstance getHazelcastInstanceByName(String instanceName) {
        return HazelcastInstanceFactory.getHazelcastInstance(instanceName);
    }

    /**
     * Gets or creates a HazelcastInstance with the default XML configuration looked up in:
     * <ol>
     *     <li>
     *         System property: Hazelcast will first check if "hazelcast.config" system property is set to a file or a
     *         {@code classpath:...} path.
     *         Examples: -Dhazelcast.config=C:/myhazelcast.xml , -Dhazelcast.config=classpath:the-hazelcast-config.xml ,
     *         -Dhazelcast.config=classpath:com/mydomain/hazelcast.xml
     *     </li>
     *     <li>
     *         "hazelcast.xml" file in current working directory
     *     </li>
     *     <li>
     *         Classpath: Hazelcast will check classpath for hazelcast.xml file.
     *     </li>
     * </ol>
     *
     * If a configuration file is not located, an {@link IllegalArgumentException} will be thrown.
     *
     * If a Hazelcast instance with the same name as the configuration exists, then it is returned, otherwise it is created.
     *
     * @return the HazelcastInstance
     * @throws IllegalArgumentException if the instance name of the config is null or empty or if no config file can be
     * located.
     */
    public static HazelcastInstance getOrCreateHazelcastInstance() {
        return HazelcastInstanceFactory.getOrCreateHazelcastInstance(null);
    }

    /**
     * Gets or creates the HazelcastInstance with a certain name.
     *
     * If a Hazelcast instance with the same name as the configuration exists, then it is returned, otherwise it is created.
     *
     * If {@code config} is {@code null}, then an XML configuration file is looked up in the following order:
     * <ol>
     *     <li>
     *         System property: Hazelcast will first check if "hazelcast.config" system property is set to a file or a
     *         {@code classpath:...} path.
     *         Examples: -Dhazelcast.config=C:/myhazelcast.xml , -Dhazelcast.config=classpath:the-hazelcast-config.xml ,
     *         -Dhazelcast.config=classpath:com/mydomain/hazelcast.xml
     *     </li>
     *     <li>
     *         "hazelcast.xml" file in current working directory
     *     </li>
     *     <li>
     *         Classpath: Hazelcast will check classpath for hazelcast.xml file.
     *     </li>
     * </ol>
     *
     * @param config the Config.
     * @return the HazelcastInstance
     * @throws IllegalArgumentException if the instance name of the config is null or empty or if no config file can be
     * located.
     */
    public static HazelcastInstance getOrCreateHazelcastInstance(Config config) {
        return HazelcastInstanceFactory.getOrCreateHazelcastInstance(config);
    }


    /**
     * Returns all active/running HazelcastInstances on this JVM.
     * <p>
     * To shutdown all running HazelcastInstances (all members on this JVM)
     * call {@link #shutdownAll()}.
     *
     * @return all active/running HazelcastInstances on this JVM
     * @see #newHazelcastInstance(Config)
     * @see #getHazelcastInstanceByName(String)
     * @see #shutdownAll()
     */
    public static Set<HazelcastInstance> getAllHazelcastInstances() {
        return HazelcastInstanceFactory.getAllHazelcastInstances();
    }

    /**
     * Sets <code>OutOfMemoryHandler</code> to be used when an <code>OutOfMemoryError</code>
     * is caught by Hazelcast threads.
     *
     * <p>
     * <b>Warning: </b> <code>OutOfMemoryHandler</code> may not be called although JVM throws
     * <code>OutOfMemoryError</code>.
     * Because error may be thrown from an external (user thread) thread
     * and Hazelcast may not be informed about <code>OutOfMemoryError</code>.
     * </p>
     *
     * @param outOfMemoryHandler set when an <code>OutOfMemoryError</code> is caught by Hazelcast threads
     *
     * @see OutOfMemoryError
     * @see OutOfMemoryHandler
     */
    public static void setOutOfMemoryHandler(OutOfMemoryHandler outOfMemoryHandler) {
        OutOfMemoryErrorDispatcher.setServerHandler(outOfMemoryHandler);
    }
}
