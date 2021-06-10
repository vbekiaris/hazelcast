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
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Factory for {@link HazelcastInstance}'s, a node in a cluster.
 */
public final class Hazelcast {

    // address -> lookups with prefix & seq
    public static final ConcurrentHashMap<Long, ConcurrentSkipListSet<DasKey>> ADDRESS_LOOKUPS = new ConcurrentHashMap<>();
    public static final ConcurrentHashMap<Long, ConcurrentSkipListSet<DasKey>> ADDRESS_INSERTIONS = new ConcurrentHashMap<>();

    public static void addAddressLookup(long prefix, long address, long seq) {
        Exception e = null;
        if (seq == 0) {
            e = new HazelcastException("seq was 0");
        }
        ADDRESS_LOOKUPS.computeIfAbsent(address, k -> new ConcurrentSkipListSet<>())
                       .add(new DasKey(prefix, address, seq, e));
    }

    public static void addAddressInsertion(long prefix, long address, long seq) {
        Exception e = null;
        if (seq == 0) {
            e = new HazelcastException("seq was 0");
        }
        ADDRESS_INSERTIONS.computeIfAbsent(address, k -> new ConcurrentSkipListSet<>())
                       .add(new DasKey(prefix, address, seq, e));
    }

    public static void dumpLookupsOf(long address) {
        System.out.println("Lookups of " + address);
        for (ConcurrentSkipListSet<DasKey> keys : ADDRESS_LOOKUPS.values()) {
            for (DasKey key : keys) {
                if (key.address == address) {
                    System.out.println("\twas looked up in prefix " + key.prefix + " with seq " + key.seq);
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

    public static void dumpInsertionsOf(long address) {
        System.out.println("Insertions of " + address);
        for (ConcurrentSkipListSet<DasKey> keys : ADDRESS_INSERTIONS.values()) {
            for (DasKey key : keys) {
                if (key.address == address) {
                    System.out.println("\twas inserted in prefix " + key.prefix + " with seq " + key.seq);
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

    public static final class DasKey implements Comparable<DasKey> {
        public final long prefix;
        public final long address;
        public final long seq;
        public final Exception exception;

        public DasKey(long prefix, long address, long seq) {
            this.prefix = prefix;
            this.address = address;
            this.seq = seq;
            this.exception = null;
        }

        public DasKey(long prefix, long address, long seq, Exception exception) {
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

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            DasKey dasKey = (DasKey) o;
            return prefix == dasKey.prefix && address == dasKey.address && seq == dasKey.seq;
        }

        @Override
        public int hashCode() {
            return Objects.hash(prefix, address, seq);
        }
    }

    public static final AtomicBoolean GC_FAILED = new AtomicBoolean();
    public static final AtomicReference<DasKey> FAILED_KEY_HANDLE = new AtomicReference();

    public static void setDasKey(long prefix, long address, long rawSeqValue) {
        FAILED_KEY_HANDLE.compareAndSet(null, new Hazelcast.DasKey(prefix, address, rawSeqValue));
    }

//    public static final ConcurrentSkipListSet<DasKey> KEY_HANDLES_ON_REPLACE_ACTIVE_CHUNK = new ConcurrentSkipListSet<>();
//
//    public static void addKHOnReplace(long address, long rawSeqValue) {
//        KEY_HANDLES_ON_REPLACE_ACTIVE_CHUNK.add(new DasKey(address, rawSeqValue));
//    }

    public static final ConcurrentSkipListSet<Long> REMOVED_KEY_ADDRESSES = new ConcurrentSkipListSet<>();

    // prefix -> key addresses that are put. Cleared by RecordStore#clear (<- reset() <- from MapReplStateHolder#applyState)
    public static final ConcurrentHashMap<Long, ConcurrentSkipListSet<Long>> PUT_KEY_ADDRESSES = new ConcurrentHashMap<>();
    public static final ConcurrentHashMap<Long, ConcurrentSkipListSet<DasKey>> PUT_DASKEY_ADDRESSES = new ConcurrentHashMap<>();

    public static void addKeyPut(long prefix, long address, long seq) {
        PUT_KEY_ADDRESSES.computeIfAbsent(prefix, k -> new ConcurrentSkipListSet<>()).add(address);
        PUT_DASKEY_ADDRESSES.computeIfAbsent(prefix, k -> new ConcurrentSkipListSet<>()).add(new DasKey(prefix, address, seq));
    }

    public static boolean containsKeyPut(long prefix, long address) {
        return PUT_KEY_ADDRESSES.computeIfAbsent(prefix, k -> new ConcurrentSkipListSet<>()).contains(address);
    }

    public static void dumpSeqsOfPuts(long prefix, long address) {
        System.out.println("Looking for PUTs of " + prefix + ", " + address);
        ConcurrentSkipListSet<DasKey> keys = PUT_DASKEY_ADDRESSES.computeIfAbsent(prefix, k -> new ConcurrentSkipListSet<>());
        for (DasKey key : keys) {
            if (key.prefix == prefix && key.address == address) {
                System.out.println("\twas put with seq " + key.seq);
            }
        }
    }

    public static void dumpSeqsOfPuts(long address) {
        System.out.println("Looking for PUTs of " + address);
        for (ConcurrentSkipListSet<DasKey> keys : PUT_DASKEY_ADDRESSES.values()) {
            for (DasKey key : keys) {
                if (key.address == address) {
                    System.out.println("\twas put in prefix " + key.prefix + " with seq " + key.seq);
                }
            }
        }
    }

    public static boolean containsKeyPut(long prefix, long address, long seq) {
        return PUT_DASKEY_ADDRESSES.computeIfAbsent(prefix, k -> new ConcurrentSkipListSet<>()).contains(
                new DasKey(prefix, address, seq));
    }

    public static void clearPutPrefix(long prefix) {
        PUT_KEY_ADDRESSES.put(prefix, new ConcurrentSkipListSet<>());
        PUT_DASKEY_ADDRESSES.put(prefix, new ConcurrentSkipListSet<>());
    }


    public static final ConcurrentSkipListSet<Long> MISSING_KEY_ADDRESSES = new ConcurrentSkipListSet<>();

    public static final ConcurrentHashMap<Long, ConcurrentSkipListSet<Long>> PREFIX_TOMBSTONES_APPLIED = new ConcurrentHashMap<>();
    // prefix -> <address -> message>
    public static final ConcurrentHashMap<Long, ConcurrentHashMap<Long, String>> PREFIX_TOMBSTONE_NOT_APPLIED = new ConcurrentHashMap<>();
    public static final ConcurrentHashMap<Long, ConcurrentHashMap<Long, String>> PREFIX_TRACKER_REMOVED = new ConcurrentHashMap<>();

    public static void addTrackerRemoved(long prefix, long address) {
        PREFIX_TRACKER_REMOVED.computeIfAbsent(prefix, k -> new ConcurrentHashMap<>()).put(address, "-");
    }

    public static void addTrackerRemoved(long prefix, long address, String message) {
        PREFIX_TRACKER_REMOVED.computeIfAbsent(prefix, k -> new ConcurrentHashMap<>()).put(address, message);
    }

    public static String containsTrackerRemoved(long prefix, long address) {
        return PREFIX_TRACKER_REMOVED.computeIfAbsent(prefix, k -> new ConcurrentHashMap<>()).get(address);
    }

    public static void clearTrackerRemoved(long[] prefixes) {
        for (long prefix : prefixes) {
            PREFIX_TRACKER_REMOVED.put(prefix, new ConcurrentHashMap<>());
        }
    }

    public static void addPrefixTmbsAppliedOn(long prefix, long address) {
        PREFIX_TOMBSTONES_APPLIED.computeIfAbsent(prefix, k -> new ConcurrentSkipListSet<>()).add(address);
    }

    public static void clearTmbsPrefixes(long[] prefixes) {
        for (long prefix : prefixes) {
            PREFIX_TOMBSTONES_APPLIED.put(prefix, new ConcurrentSkipListSet<>());
        }
    }

    public static boolean containsTmbsPrefixAppliedFor(long prefix, long address) {
        return PREFIX_TOMBSTONES_APPLIED.computeIfAbsent(prefix, k -> new ConcurrentSkipListSet<>()).contains(address);
    }

    public static void addPrefixTmbsNotAppliedOn(long prefix, long address, String message) {
        PREFIX_TOMBSTONE_NOT_APPLIED.computeIfAbsent(prefix, k -> new ConcurrentHashMap<>()).put(address, message);
    }

    public static void clearTmbsPrefixNotApplied(long[] prefixes) {
        for (long prefix : prefixes) {
            PREFIX_TOMBSTONE_NOT_APPLIED.put(prefix, new ConcurrentHashMap<>());
        }
    }

    public static String getTmbsPrefixNotAppliedFor(long prefix, long address) {
        return PREFIX_TOMBSTONE_NOT_APPLIED.computeIfAbsent(prefix, k -> new ConcurrentHashMap<>()).get(address);
    }

    private Hazelcast() {
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
