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

package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Pipelining;
import com.hazelcast.internal.partition.AntiEntropyCorrectnessTest;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.backup.BackupAccessor;
import com.hazelcast.test.backup.TestBackupUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.test.Accessors.getNode;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapAntiEntropyCorrectnessTest extends AntiEntropyCorrectnessTest {

    private static final float BACKUP_BLOCK_RATIO = 0.65f;

    @Parameterized.Parameter(2)
    public boolean merkleTreeEnabled;

    private String mapName = randomMapName();

    @Parameterized.Parameters(name = "backups:{0},nodes:{1},merkleTree:{2}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {1, 2, false},
                {1, 2, true},
                {1, InternalPartition.MAX_REPLICA_COUNT, false},
                {1, InternalPartition.MAX_REPLICA_COUNT, true},
                {2, 3, false},
                {2, 3, true},
                {2, InternalPartition.MAX_REPLICA_COUNT, false},
                {2, InternalPartition.MAX_REPLICA_COUNT, true},
                {3, 4, false},
                {3, 4, true},
                {3, InternalPartition.MAX_REPLICA_COUNT, false},
                {3, InternalPartition.MAX_REPLICA_COUNT, true},
                });
    }

    @Test
    @Override
    public void testPartitionData() {
        Config config = getConfig(false, true);
        config.getMapConfig("*")
              .setAsyncBackupCount(backupCount)
              .setBackupCount(0)
              .getMerkleTreeConfig().setEnabled(merkleTreeEnabled);
        final HazelcastInstance[] instances = factory.newInstances(config, nodeCount);
        for (HazelcastInstance instance : instances) {
            setBackupPacketDropFilter(instance, 1f);
        }
        warmUpPartitions(instances);

        HazelcastInstance driver = instances[0];
        IMap<Integer, Integer> map = driver.getMap(mapName);
        fillData(driver);

        assertEquals(10_000, map.size());

        assertBackups(instances, 10_000);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (HazelcastInstance instance : instances) {
                    InternalPartitionServiceImpl partitionService = getNode(instance).partitionService;
                    int availablePermits = partitionService.getReplicaManager().availableReplicaSyncPermits();
                    assertEquals(PARALLEL_REPLICATIONS, availablePermits);
                }
            }
        }, 10);
    }

    @Test
    public void testRemoveBackupSync() {
        Config config = getConfig(false, true);
        config.getMapConfig("*")
              .setAsyncBackupCount(backupCount)
              .setBackupCount(0)
              .getMerkleTreeConfig().setEnabled(merkleTreeEnabled);
        final HazelcastInstance[] instances = factory.newInstances(config, nodeCount);
        for (HazelcastInstance instance : instances) {
            setBackupPacketDropFilter(instance, 1f);
        }
        warmUpPartitions(instances);

        HazelcastInstance driver = instances[0];
        IMap<Integer, Integer> map = driver.getMap(mapName);
        fillData(driver);

        assertEquals(10_000, map.size());

        assertBackups(instances, 10_000);

        removeData(driver);

        assertBackups(instances, 5_000);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (HazelcastInstance instance : instances) {
                    InternalPartitionServiceImpl partitionService = getNode(instance).partitionService;
                    int availablePermits = partitionService.getReplicaManager().availableReplicaSyncPermits();
                    assertEquals(PARALLEL_REPLICATIONS, availablePermits);
                }
            }
        }, 10);
    }

    private void assertBackups(HazelcastInstance[] instances, int expectedSize) {
        assertTrueEventually(() -> {
            for (int i = 1; i <= backupCount; i++) {
                BackupAccessor backupAccessor = TestBackupUtils.newMapAccessor(instances, mapName, i);
                System.out.println("Replica index " + i + " has size " + backupAccessor.size());
                assertEquals(expectedSize, backupAccessor.size());
            }
        }, 20);
    }

    private void removeData(HazelcastInstance hz) {
        IMap<Integer, Integer> map = hz.getMap(mapName);
        for (int i = 0; i < 5_000; i++) {
            map.remove(i);
        }
    }

    private void fillData(HazelcastInstance hz) {
        try {
            IMap<Integer, Integer> map = hz.getMap(mapName);
            Pipelining<Void> pipelining = new Pipelining<>(16);
            for (int i = 0; i < 10_000; i++) {
                pipelining.add(map.setAsync(i, i));
            }
            pipelining.results();
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

}
