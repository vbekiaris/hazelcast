/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.bitset;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IBitSet;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.junit.Test;

import static org.junit.Assert.*;

public class BitSetTest extends HazelcastTestSupport {

    @Test
    public void bitsetWorks() {
        HazelcastInstance hz = createHazelcastInstance();
        IBitSet bitSet = hz.getDistributedObject(BitSetService.SERVICE_NAME, "test");
        bitSet.set(1);
        assertTrue(bitSet.get(1));
        bitSet.clear(1);
        assertFalse(bitSet.get(1));
    }

    @Test
    public void bitSetBackups() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance hz1 = factory.newHazelcastInstance();
        HazelcastInstance hz2 = factory.newHazelcastInstance();

        String name = generateKeyOwnedBy(hz1);
        IBitSet bitSet = hz1.getDistributedObject(BitSetService.SERVICE_NAME, name);
        for (int i = 0; i < 100; i++) {
            bitSet.set(i);
        }

        hz1.getLifecycleService().terminate();
        waitAllForSafeState(hz2);
        IBitSet bitSetBackup = hz2.getDistributedObject(BitSetService.SERVICE_NAME, name);
        for (int i = 0; i < 100; i++) {
            assertTrue(bitSetBackup.get(i));
        }
    }
}
