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

package com.hazelcast.util;

import com.hazelcast.core.Cluster;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberSelector;
import com.hazelcast.core.Partition;
import com.hazelcast.core.PartitionService;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.spi.OperationService;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.FutureUtil.RETHROW_ALL_EXCEPT_MEMBER_LEFT;
import static java.lang.String.format;

/**
 * Utility methods for invocations
 */
public final class InvocationUtil {
    private static final int WARMUP_SLEEPING_TIME_MILLIS = 10;

    private InvocationUtil() {

    }

    public static void invokeOnStableCluster(NodeEngine nodeEngine, OperationFactory operationFactory,
                                                 MemberSelector memberSelector, int retriesCount) {
        OperationService operationService = nodeEngine.getOperationService();
        Cluster cluster = nodeEngine.getClusterService();
        warmUpPartitions(nodeEngine);
        Collection<Member> originalMembers;
        int iterationCounter = 0;
        do {
            originalMembers = cluster.getMembers();
            Set<InternalCompletableFuture<Object>> futures = operationService.invokeOnCluster(operationFactory, memberSelector);
            FutureUtil.waitWithDeadline(futures, 1, TimeUnit.MINUTES, RETHROW_ALL_EXCEPT_MEMBER_LEFT);
            Collection<Member> currentMembers = cluster.getMembers();
            if (currentMembers.equals(originalMembers)) {
                break;
            }
            if (iterationCounter++ == retriesCount) {
                throw new HazelcastException(format("Cluster topology was not stable for %d retries,"
                        + " invoke on stable cluster failed", retriesCount));
            }
        } while (!originalMembers.equals(cluster.getMembers()));
    }

    private static void warmUpPartitions(NodeEngine nodeEngine) {
        final PartitionService ps = nodeEngine.getHazelcastInstance().getPartitionService();
        for (Partition partition : ps.getPartitions()) {
            while (partition.getOwner() == null) {
                try {
                    Thread.sleep(WARMUP_SLEEPING_TIME_MILLIS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new HazelcastException("Thread interrupted while initializing a partition table", e);
                }
            }
        }
    }
}
