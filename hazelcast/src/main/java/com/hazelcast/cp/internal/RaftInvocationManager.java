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

package com.hazelcast.cp.internal;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.exception.CannotCreateRaftGroupException;
import com.hazelcast.cp.internal.operation.ChangeRaftGroupMembershipOp;
import com.hazelcast.cp.internal.operation.DefaultRaftReplicateOp;
import com.hazelcast.cp.internal.operation.DestroyRaftGroupOp;
import com.hazelcast.cp.internal.operation.RaftQueryOp;
import com.hazelcast.cp.internal.raft.MembershipChangeMode;
import com.hazelcast.cp.internal.raft.QueryPolicy;
import com.hazelcast.cp.internal.raftop.metadata.CreateRaftGroupOp;
import com.hazelcast.cp.internal.raftop.metadata.GetActiveCPMembersOp;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.impl.Invocation;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.spi.impl.operationservice.impl.RaftInvocation;
import com.hazelcast.spi.impl.operationservice.impl.RaftInvocationContext;
import com.hazelcast.spi.properties.GroupProperty;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static com.hazelcast.cp.internal.raft.QueryPolicy.LINEARIZABLE;
import static com.hazelcast.spi.ExecutionService.ASYNC_EXECUTOR;
import static java.util.Collections.shuffle;

/**
 * Performs invocations to create & destroy Raft groups,
 * commits {@link RaftOp} and runs queries on Raft groups.
 */
@SuppressWarnings("unchecked")
public class RaftInvocationManager {

    private final NodeEngineImpl nodeEngine;
    private final OperationServiceImpl operationService;
    private final RaftService raftService;
    private final ILogger logger;
    private final RaftInvocationContext raftInvocationContext;
    private final long operationCallTimeout;
    private final int invocationMaxRetryCount;
    private final long invocationRetryPauseMillis;
    private final boolean cpSubsystemEnabled;

    RaftInvocationManager(NodeEngine nodeEngine, RaftService raftService) {
        this.nodeEngine = (NodeEngineImpl) nodeEngine;
        this.operationService = (OperationServiceImpl) nodeEngine.getOperationService();
        this.logger = nodeEngine.getLogger(getClass());
        this.raftService = raftService;
        this.raftInvocationContext = new RaftInvocationContext(logger, raftService);
        this.invocationMaxRetryCount = nodeEngine.getProperties().getInteger(GroupProperty.INVOCATION_MAX_RETRY_COUNT);
        this.invocationRetryPauseMillis = nodeEngine.getProperties().getMillis(GroupProperty.INVOCATION_RETRY_PAUSE);
        this.operationCallTimeout = nodeEngine.getProperties().getMillis(GroupProperty.OPERATION_CALL_TIMEOUT_MILLIS);
        this.cpSubsystemEnabled = raftService.getConfig().getCPMemberCount() > 0;
    }

    void reset() {
        raftInvocationContext.reset();
    }

    public CompletableFuture<RaftGroupId> createRaftGroup(String groupName) {
        return createRaftGroup(groupName, raftService.getConfig().getGroupSize());
    }

    public CompletableFuture<RaftGroupId> createRaftGroup(String groupName, int groupSize) {
        CompletableFuture<RaftGroupId> completedFuture = completeExceptionallyIfCPSubsystemNotAvailable();
        if (completedFuture != null) {
            return completedFuture;
        }

        Executor executor = nodeEngine.getExecutionService().getExecutor(ASYNC_EXECUTOR);
        CompletableFuture<RaftGroupId> resultFuture = newCompletableFuture(executor, logger);
        invokeGetMembersToCreateRaftGroup(groupName, groupSize, resultFuture);
        return resultFuture;
    }

    private <V> CompletableFuture<V> completeExceptionallyIfCPSubsystemNotAvailable() {
        if (!cpSubsystemEnabled) {
            // todo replace with HZ-specific subclass to achieve intended default executor behavior
            return newExceptionallyCompletedFuture(new HazelcastException("CP Subsystem is not enabled!"));
        }
        return null;
    }

    private void invokeGetMembersToCreateRaftGroup(String groupName, int groupSize,
                                                   CompletableFuture<RaftGroupId> resultFuture) {
        RaftOp op = new GetActiveCPMembersOp();
        CompletableFuture<List<CPMemberInfo>> f = query(raftService.getMetadataGroupId(), op, LINEARIZABLE);

        f.whenCompleteAsync((members, throwable) -> {
            if (throwable == null) {
                members = new ArrayList<>(members);

                if (members.size() < groupSize) {
                    Exception result = new IllegalArgumentException("There are not enough active members to create CP group "
                            + groupName + ". Active members: " + members.size() + ", Requested count: " + groupSize);
                    resultFuture.completeExceptionally(result);
                    return;
                }

                shuffle(members);
                members.sort(new CPMemberReachabilityComparator());
                members = members.subList(0, groupSize);
                invokeCreateRaftGroup(groupName, groupSize, members, resultFuture);
            } else {
                resultFuture.completeExceptionally(throwable);
            }
        });
    }

    private void invokeCreateRaftGroup(String groupName, int groupSize, List<CPMemberInfo> members,
                                       CompletableFuture<RaftGroupId> resultFuture) {
        CompletableFuture<RaftGroupId> f = invoke(raftService.getMetadataGroupId(), new CreateRaftGroupOp(groupName, members));

        f.whenCompleteAsync((groupId, throwable) -> {
            if (throwable == null) {
                resultFuture.complete(groupId);
            } else {
                if (throwable instanceof CannotCreateRaftGroupException) {
                    logger.fine("Could not create CP group: " + groupName + " with members: " + members,
                            throwable.getCause());
                    invokeGetMembersToCreateRaftGroup(groupName, groupSize, resultFuture);
                    return;
                }

                resultFuture.completeExceptionally(throwable);
            }
        });
    }

    <T> CompletableFuture<T> changeMembership(CPGroupId groupId, long membersCommitIndex,
                                                      CPMemberInfo member, MembershipChangeMode membershipChangeMode) {
        CompletableFuture<T> completedFuture = completeExceptionallyIfCPSubsystemNotAvailable();
        if (completedFuture != null) {
            return completedFuture;
        }
        Operation operation = new ChangeRaftGroupMembershipOp(groupId, membersCommitIndex, member, membershipChangeMode);
        Invocation invocation = new RaftInvocation(operationService.getInvocationContext(), raftInvocationContext, groupId,
                operation, invocationMaxRetryCount, invocationRetryPauseMillis, operationCallTimeout);
        return invocation.invoke().toCompletableFuture();
    }

    public <T> CompletableFuture<T> invoke(CPGroupId groupId, RaftOp raftOp) {
        CompletableFuture<T> completedFuture = completeExceptionallyIfCPSubsystemNotAvailable();
        if (completedFuture != null) {
            return completedFuture;
        }
        Operation operation = new DefaultRaftReplicateOp(groupId, raftOp);
        Invocation invocation = new RaftInvocation(operationService.getInvocationContext(), raftInvocationContext,
                groupId, operation, invocationMaxRetryCount, invocationRetryPauseMillis, operationCallTimeout);
        return invocation.invoke().toCompletableFuture();
    }

    public <T> CompletableFuture<T> query(CPGroupId groupId, RaftOp raftOp, QueryPolicy queryPolicy) {
        CompletableFuture<T> completedFuture = completeExceptionallyIfCPSubsystemNotAvailable();
        if (completedFuture != null) {
            return completedFuture;
        }
        RaftQueryOp operation = new RaftQueryOp(groupId, raftOp, queryPolicy);
        Invocation invocation = new RaftInvocation(operationService.getInvocationContext(), raftInvocationContext,
                groupId, operation, invocationMaxRetryCount, invocationRetryPauseMillis, operationCallTimeout);
        return invocation.invoke().toCompletableFuture();
    }

    public <T> CompletableFuture<T> queryLocally(CPGroupId groupId, RaftOp raftOp, QueryPolicy queryPolicy) {
        CompletableFuture<T> completedFuture = completeExceptionallyIfCPSubsystemNotAvailable();
        if (completedFuture != null) {
            return completedFuture;
        }
        RaftQueryOp operation = new RaftQueryOp(groupId, raftOp, queryPolicy);
        return nodeEngine.getOperationService().invokeOnTarget(RaftService.SERVICE_NAME, operation, nodeEngine.getThisAddress());
    }

    public CompletableFuture<Object> destroy(CPGroupId groupId) {
        CompletableFuture<Object> completedFuture = completeExceptionallyIfCPSubsystemNotAvailable();
        if (completedFuture != null) {
            return completedFuture;
        }
        Operation operation = new DestroyRaftGroupOp(groupId);
        Invocation invocation = new RaftInvocation(operationService.getInvocationContext(), raftInvocationContext,
                groupId, operation, invocationMaxRetryCount, invocationRetryPauseMillis, operationCallTimeout);
        return invocation.invoke().toCompletableFuture();
    }

    public RaftInvocationContext getRaftInvocationContext() {
        return raftInvocationContext;
    }

    private class CPMemberReachabilityComparator implements Comparator<CPMemberInfo> {
        final ClusterService clusterService = nodeEngine.getClusterService();

        @Override
        public int compare(CPMemberInfo o1, CPMemberInfo o2) {
            boolean b1 = clusterService.getMember(o1.getAddress()) != null;
            boolean b2 = clusterService.getMember(o2.getAddress()) != null;
            return b1 == b2 ? 0 : (b1 ? -1 : 1);
        }
    }

    // todo probably extract to FutureUtil or similar?
    public static CompletableFuture newCompletableFuture(Executor defaultExecutor, ILogger logger) {
        // todo implement replacement for SimpleCompletableFuture with defaultExecutor behavior
        return new CompletableFuture();
    }

    public static CompletableFuture newExceptionallyCompletedFuture(Throwable throwable) {
        CompletableFuture future = new CompletableFuture();
        future.completeExceptionally(throwable);
        return future;
    }
}
