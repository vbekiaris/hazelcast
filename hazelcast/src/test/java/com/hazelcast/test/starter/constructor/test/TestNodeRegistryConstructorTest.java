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

package com.hazelcast.test.starter.constructor.test;

import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.mocknetwork.TestNodeRegistry;
import com.hazelcast.test.starter.constructor.TestNodeRegistryConstructor;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertArrayEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class TestNodeRegistryConstructorTest {

    @Test
    public void testConstructor() throws Exception {
        Collection<Address> addresses = Arrays.asList(new Address("127.0.0.1", 5071), new Address("127.0.0.1", 5072));
        TestNodeRegistry registry = new TestNodeRegistry(addresses);

        TestNodeRegistryConstructor constructor = new TestNodeRegistryConstructor(TestNodeRegistry.class);
        TestNodeRegistry clonedVersion = (TestNodeRegistry) constructor.createNew(registry);

        // unmodifiable collections returned by getJoinAddresses() rely on Object.equals
        assertArrayEquals(registry.getJoinAddresses().toArray(new Address[0]),
                clonedVersion.getJoinAddresses().toArray(new Address[0]));
    }
}
