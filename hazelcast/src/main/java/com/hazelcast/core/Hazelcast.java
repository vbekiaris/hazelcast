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

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.config.Config;
import com.hazelcast.instance.impl.HazelcastBootstrap;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.instance.impl.OutOfMemoryErrorDispatcher;
import com.hazelcast.jet.config.JobConfig;

import javax.annotation.Nonnull;
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

    /** checkstyle comment */
    public static final ConcurrentHashMap<Long, ConcurrentSkipListSet<String>> ADDRESS_HISTORY
            = new ConcurrentHashMap<>();

    private Hazelcast() {
    }

    public static void addRemovedKey(long prefix, long address, long seq) {
        Exception e = null;
        if (seq == 0) {
            e = new HazelcastException("seq was 0");
        }
        DasKey key = new DasKey(prefix, address, seq, e);
        REMOVED_KEYS_BY_PREFIX.computeIfAbsent(prefix, k -> new ConcurrentSkipListSet<>())
                       .add(key);
        ADDRESS_HISTORY.computeIfAbsent(prefix, k -> new ConcurrentSkipListSet<>())
                       .add("Removed by removeSpecial: " + key);
    }

    public static void addRemovedKey(String reason, long prefix, long address, long seq) {
        // no op
//        Exception e = null;
//        if (seq == 0) {
//            e = new HazelcastException("seq was 0");
//        }
//        ConcurrentSkipListSet set = ADDRESS_HISTORY.computeIfAbsent(address, k -> new ConcurrentSkipListSet<>());
//        set.add(set.size() + " - " + reason + ": " + new DasKey(prefix, address, seq, e));
    }

    public static void clearRemovedKeys(long prefix) {
        REMOVED_KEYS_BY_PREFIX.put(prefix, new ConcurrentSkipListSet<>());
    }

    public static void dumpRemovalsOf(long address) {
        ConcurrentSkipListSet<String> removalReasons = ADDRESS_HISTORY.get(address);
        if (removalReasons != null) {
            System.out.println(">> Removals of address " + address);
            for (String reason : removalReasons) {
                System.out.println("\t" + reason);
            }
        } else {
            System.out.println(">> No removals of address " + address);
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
     * Returns either a local Hazelcast instance or a "bootstrapped" Hazelcast
     * client for a remote Hazelcast cluster, depending on the context. The main
     * goal of this factory method is to simplify submitting a Jet job to a remote
     * Hazelcast cluster while also making it convenient to test on the local
     * machine.
     * <p>
     * When you submit a job to a Hazelcast instance that runs locally in your
     * JVM, it will have all the dependency classes available. However, when
     * you take the same job to a remote Hazelcast cluster, you'll often find
     * that it fails with a {@code ClassNotFoundException} because the remote
     * cluster doesn't have all the classes you use in the job.
     * <p>
     * Normally you would have to explicitly add all the dependency classes to
     * the {@link JobConfig#addClass(Class[]) JobConfig}, either one by one or
     * packaged into a JAR. If you're submitting a job using the command-line
     * tool {@code hazelcast submit}, the JAR to attach is the same JAR that
     * contains the code that submits the job.
     * <p>
     * This factory takes all of the above into account in order to provide a
     * smoother experience:
     * <ul><li>
     *     When not called from {@code hazelcast submit}, it returns a local {@code
     *     HazelcastInstance}, either by creating a new one or looking up a cached one.
     *     The instance won't join any cluster.
     * <li>
     *     When called from {@code hazelcast submit}, it returns a "bootstrapped"
     *     instance of Hazelcast client that automatically attaches the JAR to all jobs
     *     you submit using it.
     * </ul>
     * With these semantics in place it's simple to write code that works both
     * in your local development/testing environment (using a local Hazelcast
     * instance) and in production (using the remote cluster).
     * <p>
     * To use this feature, follow these steps:
     * <ol><li>
     *     Write your {@code main()} method and your code the usual way, making
     *     sure you use this method (instead of {@link HazelcastClient#newHazelcastClient()})
     *     to acquire a Hazelcast client instance.
     * <li>
     *     Create a runnable JAR (e.g. {@code jet-job.jar}) with your entry point
     *     declared as the {@code Main-Class} in {@code MANIFEST.MF}. The JAR should
     *     include all dependencies required to run it (except the Hazelcast classes
     *     &mdash; these are already available on the cluster classpath).
     * <li>
     *     Submit the job by writing {@code hazelcast submit hz-job.jar} on
     *     the command line. This assumes you have downloaded the Hazelcast
     *     distribution package and its {@code bin} directory is on your system
     *     path. The Hazelcast client will use the configuration file {@code
     *     <distro_root>/config/hazelcast-client.yaml}. Adjust that file as
     *     needed.
     * <li>
     *     The same code will work if you run it directly from your IDE. In this
     *     case it will create a local Hazelcast instance for itself to run on.
     * </ol>
     * For example, you can write a class like this:
     * <pre>
     * public class CustomJetJob {
     *   public static void main(String[] args) {
     *     HazelcastInstance hz = Hazelcast.bootstrappedInstance();
     *     hz.getJet().newJob(buildPipeline()).join();
     *   }
     *
     *   public static Pipeline buildPipeline() {
     *       // ...
     *   }
     * }
     * </pre>
     *
     * @since 5.0
     */
    @Nonnull
    public static HazelcastInstance bootstrappedInstance() {
        return HazelcastBootstrap.getInstance();
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
