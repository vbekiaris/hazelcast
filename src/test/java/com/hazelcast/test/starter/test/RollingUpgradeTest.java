/*
 * Copyright (c) 2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.test.starter.test;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.starter.HazelcastStarter;
import org.junit.Test;

import static com.hazelcast.test.HazelcastTestSupport.assertClusterSizeEventually;
import static org.junit.Assert.assertEquals;

public class RollingUpgradeTest {

    @Test
    public void testAllVersions() {
        String[] versions = new String[]{"3.7", "3.7.1", "3.7.2", "3.7.3", "3.7.4", "3.7.5"};
        HazelcastInstance[] instances = new HazelcastInstance[versions.length];
        for (int i = 0; i < versions.length; i++) {
            instances[i] = HazelcastStarter.startHazelcastVersion(versions[i]);
        }
        assertClusterSizeEventually(6, instances[0]);
        for (HazelcastInstance hz : instances) {
            hz.shutdown();
        }
    }

    @Test
    public void unitTestIsTheUI_forRealProgrammers() {
        HazelcastInstance hz374 = HazelcastStarter.startHazelcastVersion("3.7.4");
        HazelcastInstance hz375 = HazelcastStarter.startHazelcastVersion("3.7.5");

        IMap<Integer, String> map374 = hz374.getMap("myMap");
        map374.put(42, "GUI = Cheating!");

        IMap<Integer, String> myMap = hz375.getMap("myMap");
        String ancientWisdom = myMap.get(42);

        assertEquals("GUI = Cheating!", ancientWisdom);
        hz374.shutdown();
        hz375.shutdown();
    }
}
