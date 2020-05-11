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

package com.hazelcast.map.impl;

import com.hazelcast.config.MapConfig;
import com.hazelcast.internal.nearcache.impl.invalidation.MetaDataGenerator;
import com.hazelcast.internal.partition.FragmentedMigrationAwareService;
import com.hazelcast.internal.partition.MigrationEndpoint;
import com.hazelcast.internal.partition.PartitionMigrationEvent;
import com.hazelcast.internal.partition.PartitionReplicationEvent;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.services.ObjectNamespace;
import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.map.impl.operation.MapReplicationOperation;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.publisher.PublisherContext;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.Records;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.map.impl.recordstore.RecordStoreAdapter;
import com.hazelcast.query.impl.Index;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.query.impl.InternalIndex;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.util.Collection;
import java.util.Map;
import java.util.Stack;
import java.util.function.Predicate;

import static com.hazelcast.cluster.ClusterState.STABLE_CLUSTER;
import static com.hazelcast.internal.partition.MigrationEndpoint.DESTINATION;
import static com.hazelcast.internal.partition.MigrationEndpoint.SOURCE;
import static com.hazelcast.map.impl.querycache.publisher.AccumulatorSweeper.flushAccumulator;
import static com.hazelcast.map.impl.querycache.publisher.AccumulatorSweeper.removeAccumulator;
import static com.hazelcast.map.impl.querycache.publisher.AccumulatorSweeper.sendEndOfSequenceEvents;

/**
 * Defines migration behavior of map service.
 *
 * @see MapService
 */
class MapMigrationAwareService implements FragmentedMigrationAwareService {

    private final PartitionContainer[] containers;
    private final MapServiceContext mapServiceContext;
    private final SerializationService serializationService;
    private final Stack<String> promotions;

    MapMigrationAwareService(MapServiceContext mapServiceContext) {
        this.mapServiceContext = mapServiceContext;
        this.serializationService = mapServiceContext.getNodeEngine().getSerializationService();
        this.containers = mapServiceContext.getPartitionContainers();
        this.promotions = new Stack<>();
    }

    @Override
    public Collection<ServiceNamespace> getAllServiceNamespaces(PartitionReplicationEvent event) {
        return containers[event.getPartitionId()].getAllNamespaces(event.getReplicaIndex());
    }

    @Override
    public boolean isKnownServiceNamespace(ServiceNamespace namespace) {
        return namespace instanceof ObjectNamespace
                && MapService.SERVICE_NAME.equals(namespace.getServiceName());
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent event) {
        if (isLocalPromotion(event)) {
            // It's a local partition promotion. We need to populate non-global indexes here since
            // there is no map replication performed in this case. Global indexes are populated
            // during promotion finalization phase.

            // 1. Defensively clear possible stale leftovers from the previous failed promotion attempt.
            clearNonGlobalIndexes(event);

            // 2. Populate non-global partitioned indexes.
            populateIndexes(event, TargetIndexes.NON_GLOBAL);

            // 3. TODO: Hot-Restart hook: mark beginning of promotion
            hookHotRestartMutationObserver(event);
        }

        flushAndRemoveQueryCaches(event);
    }

    private void hookHotRestartMutationObserver(PartitionMigrationEvent event) {
        for (Map.Entry<String, MapContainer> mapContainerEntry : mapServiceContext.getMapContainers().entrySet()) {
            String mapName = mapContainerEntry.getKey();
            MapConfig mapConfig = mapContainerEntry.getValue().getMapConfig();
            if (mapConfig.getHotRestartConfig().isEnabled()
                    && STABLE_CLUSTER == mapServiceContext.getNodeEngine().getClusterService().getClusterState()) {
                mapServiceContext.getRecordStore(event.getPartitionId(), mapName)
                                 .markPromotion();
            }
        }
    }

    /**
     * Flush and remove query cache on this source partition.
     */
    private void flushAndRemoveQueryCaches(PartitionMigrationEvent event) {
        int partitionId = event.getPartitionId();
        QueryCacheContext queryCacheContext = mapServiceContext.getQueryCacheContext();
        PublisherContext publisherContext = queryCacheContext.getPublisherContext();

        if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
            flushAccumulator(publisherContext, partitionId);
            removeAccumulator(publisherContext, partitionId);
            return;
        }

        if (isLocalPromotion(event)) {
            removeAccumulator(publisherContext, partitionId);
            sendEndOfSequenceEvents(publisherContext, partitionId);
            return;
        }
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        return prepareReplicationOperation(event,
                containers[event.getPartitionId()].getAllNamespaces(event.getReplicaIndex()));
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event,
                                                 Collection<ServiceNamespace> namespaces) {
        assert assertAllKnownNamespaces(namespaces);

        int partitionId = event.getPartitionId();


        // a differential update is possible if:
        // 1) cluster version >= 4.1,
        // 2) hot restart is enabled for a namespace
        // 3) migration event has replicaIndex == 0
        // 1 & 2 are checked in MapReplicationStateHolder at the time of gathering data for
        // replication, while 3 is checked here.
        Operation operation = new MapReplicationOperation(containers[partitionId],
                namespaces, partitionId, event.getReplicaIndex(), differentialMigrationHint(event));
        operation.setService(mapServiceContext.getService());
        operation.setNodeEngine(mapServiceContext.getNodeEngine());

        return operation;
    }

    private boolean differentialMigrationHint(PartitionReplicationEvent e) {
        return e.getReplicaIndex() == 0;
    }

    private boolean assertAllKnownNamespaces(Collection<ServiceNamespace> namespaces) {
        for (ServiceNamespace namespace : namespaces) {
            assert isKnownServiceNamespace(namespace) : namespace + " is not a MapService namespace!";
        }
        return true;
    }

    @Override
    public void commitMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == DESTINATION) {
            populateIndexes(event, TargetIndexes.GLOBAL);
        } else {
            depopulateIndexes(event);
        }

        if (SOURCE == event.getMigrationEndpoint()) {
            // Do not change order of below methods
            removeWbqCountersHavingLesserBackupCountThan(event.getPartitionId(),
                    event.getNewReplicaIndex());
            removeRecordStoresHavingLesserBackupCountThan(event.getPartitionId(),
                    event.getNewReplicaIndex());
        }

        PartitionContainer partitionContainer
                = mapServiceContext.getPartitionContainer(event.getPartitionId());
        for (RecordStore recordStore : partitionContainer.getAllRecordStores()) {
            // in case the record store has been created without loading during migration trigger again
            // if loading has been already started this call will do nothing
            recordStore.startLoading();
        }
        mapServiceContext.reloadOwnedPartitions();

        removeOrRegenerateNearCacheUuid(event);
    }

    private void removeOrRegenerateNearCacheUuid(PartitionMigrationEvent event) {
        if (SOURCE == event.getMigrationEndpoint()) {
            getMetaDataGenerator().removeUuidAndSequence(event.getPartitionId());
            return;
        }

        if (DESTINATION == event.getMigrationEndpoint() && event.getNewReplicaIndex() != 0) {
            getMetaDataGenerator().regenerateUuid(event.getPartitionId());
            return;
        }
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {
        if (DESTINATION == event.getMigrationEndpoint()) {
            // Do not change order of below methods
            removeWbqCountersHavingLesserBackupCountThan(event.getPartitionId(),
                    event.getCurrentReplicaIndex());
            removeRecordStoresHavingLesserBackupCountThan(event.getPartitionId(),
                    event.getCurrentReplicaIndex());
            getMetaDataGenerator().removeUuidAndSequence(event.getPartitionId());
        }

        mapServiceContext.reloadOwnedPartitions();
    }

    private void clearNonGlobalIndexes(PartitionMigrationEvent event) {
        final PartitionContainer container = mapServiceContext.getPartitionContainer(event.getPartitionId());
        for (RecordStore recordStore : container.getMaps().values()) {
            final MapContainer mapContainer = mapServiceContext.getMapContainer(recordStore.getName());

            final Indexes indexes = mapContainer.getIndexes(event.getPartitionId());
            if (!indexes.haveAtLeastOneIndex() || indexes.isGlobal()) {
                // no indexes to work with
                continue;
            }

            indexes.clearAll();
        }
    }

    private void removeRecordStoresHavingLesserBackupCountThan(int partitionId, int thresholdReplicaIndex) {
        if (thresholdReplicaIndex < 0) {
            mapServiceContext.removeRecordStoresFromPartitionMatchingWith(recordStore -> true, partitionId,
                    false, true);
        } else {
            mapServiceContext.removeRecordStoresFromPartitionMatchingWith(lesserBackupMapsThen(thresholdReplicaIndex),
                    partitionId, false, true);
        }
    }

    /**
     * Removes write-behind-queue-reservation-counters inside
     * supplied partition from matching record-stores.
     */
    private void removeWbqCountersHavingLesserBackupCountThan(int partitionId, int thresholdReplicaIndex) {
        if (thresholdReplicaIndex < 0) {
            mapServiceContext.removeWbqCountersFromMatchingPartitionsWith(
                    recordStore -> true, partitionId);
        } else {
            mapServiceContext.removeWbqCountersFromMatchingPartitionsWith(
                    lesserBackupMapsThen(thresholdReplicaIndex), partitionId);
        }
    }

    /**
     * @param backupCount number of backups of a maps' partition
     * @return predicate to find all map partitions which are expected to have
     * lesser backups than given backupCount.
     */
    private static Predicate<RecordStore> lesserBackupMapsThen(final int backupCount) {
        return recordStore -> recordStore.getMapContainer().getTotalBackupCount() < backupCount;
    }

    private MetaDataGenerator getMetaDataGenerator() {
        return mapServiceContext.getMapNearCacheManager().getInvalidator().getMetaDataGenerator();
    }

    private void populateIndexes(PartitionMigrationEvent event, TargetIndexes targetIndexes) {
        assert event.getMigrationEndpoint() == DESTINATION;
        assert targetIndexes != null;

        if (event.getNewReplicaIndex() != 0) {
            // backup partitions have no indexes to populate
            return;
        }

        final PartitionContainer container = mapServiceContext.getPartitionContainer(event.getPartitionId());
        for (RecordStore<Record> recordStore : container.getMaps().values()) {
            final MapContainer mapContainer = mapServiceContext.getMapContainer(recordStore.getName());
            final StoreAdapter storeAdapter = new RecordStoreAdapter(recordStore);

            final Indexes indexes = mapContainer.getIndexes(event.getPartitionId());
            indexes.createIndexesFromRecordedDefinitions(storeAdapter);
            if (!indexes.haveAtLeastOneIndex()) {
                // no indexes to work with
                continue;
            }

            if (indexes.isGlobal() && targetIndexes == TargetIndexes.NON_GLOBAL) {
                continue;
            }
            if (!indexes.isGlobal() && targetIndexes == TargetIndexes.GLOBAL) {
                continue;
            }

            InternalIndex[] indexesSnapshot = indexes.getIndexes();

            recordStore.forEach((key, record) -> {
                Object value = Records.getValueOrCachedValue(record, serializationService);
                if (value != null) {
                    QueryableEntry queryEntry = mapContainer.newQueryEntry(key, value);
                    queryEntry.setRecord(record);
                    queryEntry.setStoreAdapter(storeAdapter);
                    indexes.putEntry(queryEntry, null, Index.OperationSource.SYSTEM);
                }
            }, false);

            Indexes.markPartitionAsIndexed(event.getPartitionId(), indexesSnapshot);
        }
    }

    private void depopulateIndexes(PartitionMigrationEvent event) {
        assert event.getMigrationEndpoint() == SOURCE;
        assert event.getNewReplicaIndex() != 0 : "Invalid migration event: " + event;

        if (event.getCurrentReplicaIndex() != 0) {
            // backup partitions have no indexes to depopulate
            return;
        }

        final PartitionContainer container = mapServiceContext.getPartitionContainer(event.getPartitionId());
        for (RecordStore<Record> recordStore : container.getMaps().values()) {
            final MapContainer mapContainer = mapServiceContext.getMapContainer(recordStore.getName());

            final Indexes indexes = mapContainer.getIndexes(event.getPartitionId());
            if (!indexes.haveAtLeastOneIndex()) {
                // no indexes to work with
                continue;
            }

            InternalIndex[] indexesSnapshot = indexes.getIndexes();

            recordStore.forEach((key, record) -> {
                Object value = Records.getValueOrCachedValue(record, serializationService);
                indexes.removeEntry(key, value, Index.OperationSource.SYSTEM);
            }, false);

            Indexes.markPartitionAsUnindexed(event.getPartitionId(), indexesSnapshot);
        }
    }

    private enum TargetIndexes {
        GLOBAL, NON_GLOBAL
    }

    private static boolean isLocalPromotion(PartitionMigrationEvent event) {
        return event.getMigrationEndpoint() == DESTINATION && event.getCurrentReplicaIndex() > 0
                && event.getNewReplicaIndex() == 0;
    }

    protected long getNow() {
        return Clock.currentTimeMillis();
    }
}
