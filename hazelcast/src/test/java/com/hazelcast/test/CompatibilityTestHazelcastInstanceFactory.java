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

package com.hazelcast.test;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.NodeContext;
import com.hazelcast.nio.Address;
import com.hazelcast.test.starter.HazelcastStarter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.instance.BuildInfoProvider.HAZELCAST_INTERNAL_OVERRIDE_VERSION;

/**
 * A TestHazelcastInstanceFactory to be used to create HazelcastInstances in compatibility tests.
 */
public class CompatibilityTestHazelcastInstanceFactory extends TestHazelcastInstanceFactory {

    private static final String[] VERSIONS = new String[] {"3.8", "3.8", "3.8"};
    private static final String CURRENT_VERSION = "";

    // keep track of number of created instances
    private final AtomicInteger instancesCreated = new AtomicInteger();
    // todo shouldn't be necessary after all
    private final ArrayList<HazelcastInstance> instances = new ArrayList<HazelcastInstance>();

    public CompatibilityTestHazelcastInstanceFactory() {
    }

    public CompatibilityTestHazelcastInstanceFactory(int count) {
        super(count);
    }

    public CompatibilityTestHazelcastInstanceFactory(String... addresses) {
        super(addresses);
    }

    public CompatibilityTestHazelcastInstanceFactory(int initialPort, String... addresses) {
        super(initialPort, addresses);
    }

    public CompatibilityTestHazelcastInstanceFactory(Collection<Address> addresses) {
        super(addresses);
    }

    @Override
    public HazelcastInstance newHazelcastInstance() {
        return nextInstance();
    }

    @Override
    public HazelcastInstance newHazelcastInstance(Config config) {
        return nextInstance(config);
    }

    @Override
    public HazelcastInstance newHazelcastInstance(Address address) {
        return super.newHazelcastInstance(address);
    }

    @Override
    public HazelcastInstance newHazelcastInstance(Address address, Config config) {
        return nextInstance(config);
    }

    @Override
    public HazelcastInstance newHazelcastInstance(Config config, Address[] blockedAddresses) {
        return super.newHazelcastInstance(config, blockedAddresses);
    }

    @Override
    public HazelcastInstance newHazelcastInstance(Address address, Config config,
                                                  Address[] blockedAddresses) {
        return super.newHazelcastInstance(address, config, blockedAddresses);
    }

    @Override
    public HazelcastInstance[] newInstances() {
        return super.newInstances();
    }

    @Override
    public HazelcastInstance[] newInstances(Config config, int nodeCount) {
        return super.newInstances(config, nodeCount);
    }

    @Override
    public HazelcastInstance[] newInstances(Config config) {
        return super.newInstances(config);
    }

    // return the version of the next instance to be created
    private String nextVersion() {
        if (instancesCreated.get() >= VERSIONS.length) {
            return CURRENT_VERSION;
        }
        try {
            return VERSIONS[instancesCreated.getAndIncrement()];
        } catch (ArrayIndexOutOfBoundsException e) {
            return CURRENT_VERSION;
        }
    }

    private HazelcastInstance nextInstance() {
        String nextVersion = nextVersion();
        if (nextVersion == CURRENT_VERSION) {
            return super.newHazelcastInstance((Config) null);
        } else {
            return HazelcastStarter.newHazelcastInstance(nextVersion);
        }
    }

    private HazelcastInstance nextInstance(Config config) {
        String nextVersion = nextVersion();
        if (nextVersion == CURRENT_VERSION) {
            System.setProperty(HAZELCAST_INTERNAL_OVERRIDE_VERSION, "3.8");
            HazelcastInstance hz = super.newHazelcastInstance(config);
            System.clearProperty(HAZELCAST_INTERNAL_OVERRIDE_VERSION);
            instances.add(hz);
            return hz;
        } else {
            HazelcastInstance hz;
            NodeContext nodeContext = TestEnvironment.isMockNetwork() ? registry.createNodeContext(nextAddress())
                    : null;
            hz = HazelcastStarter.newHazelcastInstance(nextVersion, config, nodeContext);
            instances.add(hz);
            return hz;
        }
    }

    @Override
    public void shutdownAll() {
        for (HazelcastInstance hz : instances) {
            hz.shutdown();
        }
    }

    @Override
    public void terminateAll() {
        for (HazelcastInstance hz : instances) {
            hz.getLifecycleService().terminate();
        }
    }
}
