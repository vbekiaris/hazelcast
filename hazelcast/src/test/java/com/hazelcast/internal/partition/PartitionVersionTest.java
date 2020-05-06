/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.partition;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Pipelining;
import com.hazelcast.internal.services.DistributedObjectNamespace;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.test.Accessors;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PartitionVersionTest extends HazelcastTestSupport {

    @Test
    public void testVersions()
            throws InterruptedException {
        HazelcastInstance[] instances = createHazelcastInstanceFactory(2).newInstances();
        IMap<String, String> map = instances[0].getMap("test");
        Pipelining<Void> pipelining = new Pipelining<>(32);
        for (int i = 0; i < 100_000; i++) {
            if (i % 5_000 == 0) {
                System.out.println("Added " + i);
            }
            pipelining.add(map.setAsync(""+i, ""+i));
        }

        InternalPartitionService partitionService0 = Accessors.getPartitionService(instances[0]);
        InternalPartitionService partitionService1 = Accessors.getPartitionService(instances[1]);

        int partitionCount = 271;
        for (int i = 0; i < partitionCount; i++) {
            System.out.println(">> "
                    + Arrays.toString(partitionService0.getPartitionReplicaVersionManager()
                                                                .getPartitionReplicaVersions(i,
                                                                new DistributedObjectNamespace(MapService.SERVICE_NAME, "test"))));

        }

    }
}
