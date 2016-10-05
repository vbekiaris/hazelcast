/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.task.map;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.task.AbstractCallableMessageTask;
import com.hazelcast.core.Member;
import com.hazelcast.instance.Node;
import com.hazelcast.map.QueryResultSizeExceededException;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.query.QueryOperation;
import com.hazelcast.map.impl.query.QueryPartitionOperation;
import com.hazelcast.map.impl.query.QueryResult;
import com.hazelcast.map.impl.query.QueryResultRow;
import com.hazelcast.nio.Connection;
import com.hazelcast.query.Predicate;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.util.BitSetUtils;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.IterationType;

import java.security.Permission;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.logging.Level;

import static com.hazelcast.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;
import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.util.BitSetUtils.hasAtLeastOneBitSet;
import static com.hazelcast.util.ExceptionUtil.rethrow;

public abstract class AbstractMapQueryMessageTask<P> extends AbstractCallableMessageTask<P> {

    private final ConcurrentHashMap<String, List<String>> executionLog = new ConcurrentHashMap<String, List<String>>();
    private boolean shouldLog = false;

    protected AbstractMapQueryMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    public final String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new MapPermission(getDistributedObjectName(), ActionConstants.ACTION_READ);
    }

    protected abstract Predicate getPredicate();

    protected abstract IterationType getIterationType();

    protected abstract Object reduce(Collection<QueryResultRow> result);

    @Override
    protected final Object call() throws Exception {
        Collection<QueryResultRow> result = new LinkedList<QueryResultRow>();
        try {
            Predicate predicate = getPredicate();
            int partitionCount = clientEngine.getPartitionService().getPartitionCount();

            BitSet finishedPartitions = invokeOnMembers(result, predicate, partitionCount);
            invokeOnMissingPartitions(result, predicate, finishedPartitions, partitionCount);
            if (shouldLog || result.size() != 1000) {
                System.out.println("Actual result size is " + result.size() + " instead of expected 1000.");
                dumpLog();
            }
            List<Integer> missingList = findMissingPartitions(finishedPartitions, partitionCount);
            if (missingList.size() > 0) {
                System.out.println("Queries completed but still missing results for partitions " + join(missingList, ","));
        }
        } catch (Throwable t) {
            throw rethrow(t);
        }
        return reduce(result);
    }

    private BitSet invokeOnMembers(Collection<QueryResultRow> result, Predicate predicate, int partitionCount)
            throws InterruptedException, ExecutionException {
        Collection<Member> members = clientEngine.getClusterService().getMembers(DATA_MEMBER_SELECTOR);
        List<Future> futures = createInvocations(members, predicate);
        return collectResults(result, futures, partitionCount);
    }

    private void invokeOnMissingPartitions(Collection<QueryResultRow> result, Predicate predicate,
                                           BitSet finishedPartitions, int partitionCount)
            throws InterruptedException, java.util.concurrent.ExecutionException {
        if (hasMissingPartitions(finishedPartitions, partitionCount)) {
            List<Integer> missingList = findMissingPartitions(finishedPartitions, partitionCount);
            List<Future> missingFutures = new ArrayList<Future>(missingList.size());
            createInvocationsForMissingPartitions(missingList, missingFutures, predicate);
            collectResultsFromMissingPartitions(finishedPartitions, result, missingFutures);
        }
    }

    private List<Future> createInvocations(Collection<Member> members, Predicate predicate) {
        List<Future> futures = new ArrayList<Future>(members.size());
        final InternalOperationService operationService = nodeEngine.getOperationService();
        for (Member member : members)
            try {
                Future future = operationService.createInvocationBuilder(SERVICE_NAME,
                        new QueryOperation(getDistributedObjectName(), predicate, getIterationType()), member.getAddress())
                                                .invoke();
                futures.add(future);
            } catch (Throwable t) {
                if (t.getCause() instanceof QueryResultSizeExceededException) {
                    rethrow(t);
                } else {
                    // log failure to invoke query on member at fine level
                    // the missing partition IDs will be queried anyway, so it's not a terminal failure
                    if (logger.isFineEnabled()) {
                        logger.log(Level.FINE, "Could not invoke query on member " + member, t);
                    }
                }
            }
        return futures;
    }

    @SuppressWarnings("unchecked")
    private BitSet collectResults(Collection<QueryResultRow> result, List<Future> futures, int partitionCount)
            throws InterruptedException, ExecutionException {
        BitSet finishedPartitions = new BitSet(partitionCount);
        for (Future future : futures) {
            try {
                QueryResult queryResult = (QueryResult) future.get();
                logPartitionsQueried(queryResult);
                if (queryResult != null) {
                    Collection<Integer> partitionIds = queryResult.getPartitionIds();
                    if (partitionIds != null && !hasAtLeastOneBitSet(finishedPartitions, partitionIds)) {
                        //Collect results only if there is no overlap with already collected partitions.
                        //If there is an overlap it means there was a partition migration while QueryOperation(s) were
                        //running. In this case we discard all results from this member and will target the missing
                        //partition separately later.
                        BitSetUtils.setBits(finishedPartitions, partitionIds);
                        result.addAll(queryResult.getRows());
                    } else {
                        putOrAppend(executionLog, Thread.currentThread().getName(), "Not taking into account partition IDs"
                                + ": " + join(partitionIds, ","));
                    }
                }
            }
            catch (Throwable t) {
                if (t.getCause() instanceof QueryResultSizeExceededException) {
                    rethrow(t);
                } else {
                    // log failure to invoke query on member at fine level
                    // the missing partition IDs will be queried anyway, so it's not a terminal failure
                    if (logger.isFineEnabled()) {
                        logger.log(Level.FINE, "Query on member failed with exception", t);
                    }
                }
            }
        }
        return finishedPartitions;
    }

    private boolean hasMissingPartitions(BitSet finishedPartitions, int partitionCount) {
        return finishedPartitions.nextClearBit(0) < partitionCount;
    }

    private List<Integer> findMissingPartitions(BitSet finishedPartitions, int partitionCount) {
        List<Integer> missingList = new ArrayList<Integer>();
        for (int i = 0; i < partitionCount; i++) {
            if (!finishedPartitions.get(i)) {
                missingList.add(i);
            }
        }
        return missingList;
    }

    private void createInvocationsForMissingPartitions(List<Integer> missingPartitionsList, List<Future> futures,
                                                       Predicate predicate) {

        final InternalOperationService operationService = nodeEngine.getOperationService();
        for (Integer partitionId : missingPartitionsList) {
            QueryPartitionOperation queryPartitionOperation = new QueryPartitionOperation(
                    getDistributedObjectName(), predicate, getIterationType());
            queryPartitionOperation.setPartitionId(partitionId);
            try {
                Future future = operationService.invokeOnPartition(SERVICE_NAME,
                        queryPartitionOperation, partitionId);
                futures.add(future);
            } catch (Throwable t) {
                throw rethrow(t);
            }
        }
    }

    private void collectResultsFromMissingPartitions(BitSet finishedPartitions, Collection<QueryResultRow> result, List<Future> futures)
            throws InterruptedException, java.util.concurrent.ExecutionException {
        for (Future future : futures) {
            QueryResult queryResult = (QueryResult) future.get();
            if (queryResult.getPartitionIds() != null && queryResult.getPartitionIds().size() > 0 &&
                    !hasAtLeastOneBitSet(finishedPartitions, queryResult.getPartitionIds())) {
                logPartitionsQueried(queryResult);
                result.addAll(queryResult.getRows());
                BitSetUtils.setBits(finishedPartitions, queryResult.getPartitionIds());
            } else {
                shouldLog = true;
                putOrAppend(executionLog, queryResult.getThreadName(),
                        "A QueryPartitionOp returned with a partition ID which has already been"
                        + " queried: " +  join(queryResult.getPartitionIds(), ","));
            }
        }
    }

    private void dumpLog() {
        for (Map.Entry<String, List<String>> entry : executionLog.entrySet()) {
            for (String s : entry.getValue()) {
                System.out.println("[" + Thread.currentThread() +"] "+ entry.getKey() + ": " + s);
            }
        }
    }

    private void logPartitionsQueried(QueryResult resultRows) {
        String partitions;
        if (resultRows.getPartitionIds() != null) {
            if (resultRows.getPartitionIds().size() == 0) {
                partitions = "EMPTY";
            } else {
                partitions = join(resultRows.getPartitionIds(), ",");
            }
        } else {
            partitions = "N/A";
        }
        putOrAppend(executionLog, resultRows.getThreadName(), (resultRows.getRows() == null ? 0 : resultRows.getRows().size()) +
                " from partition IDs " + partitions);
    }

    public static String join(Iterable<?> iterable, String separator) {
        if(separator == null) {
            separator = "";
        }

        if (iterable == null) {
            return "";
        }

        Iterator<?> iterator = iterable.iterator();

        StringBuilder buf = new StringBuilder(256);

        while (iterator.hasNext()) {
            buf.append(iterator.next());
            if(iterator.hasNext()) {
                buf.append(separator);
            }
        }

        return buf.toString();
    }

    public void putOrAppend(ConcurrentHashMap<String, List<String>> log, String key, String message) {
        if (log.containsKey(key)) {
            log.get(key).add(message);
        } else {
            log.putIfAbsent(key, new ArrayList<String>());
            log.get(key).add(message);
        }
    }
}
