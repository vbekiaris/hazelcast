package com.hazelcast.map.impl;

import com.hazelcast.config.MapConfig;
import com.hazelcast.core.IFunction;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.map.impl.eviction.Evictor;
import com.hazelcast.map.impl.mapstore.MapStoreContext;
import com.hazelcast.map.impl.record.RecordFactory;
import com.hazelcast.map.merge.MapMergePolicy;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.wan.WanReplicationPublisher;

/**
 *
 */
public interface IMapContainer {
    void initWanReplication(NodeEngine nodeEngine);

    Indexes getIndexes();

    WanReplicationPublisher getWanReplicationPublisher();

    MapMergePolicy getWanMergePolicy();

    boolean isWanReplicationEnabled();

    void checkWanReplicationQueues();

    boolean hasMemberNearCache();

    int getTotalBackupCount();

    int getBackupCount();

    int getAsyncBackupCount();

    PartitioningStrategy getPartitioningStrategy();

    SizeEstimator getNearCacheSizeEstimator();

    MapServiceContext getMapServiceContext();

    MapStoreContext getMapStoreContext();

    MapConfig getMapConfig();

    void setMapConfig(MapConfig mapConfig);

    String getName();

    String getQuorumName();

    IFunction<Object, Data> toData();

    ConstructorFunction<Void, RecordFactory> getRecordFactoryConstructor();

    QueryableEntry newQueryEntry(Data key, Object value);

    Evictor getEvictor();

    // only used for testing purposes.
    void setEvictor(Evictor evictor);

    boolean isMemberNearCacheInvalidationEnabled();

    boolean hasInvalidationListener();

    void increaseInvalidationListenerCount();

    void decreaseInvalidationListenerCount();

    boolean isInvalidationEnabled();

    InterceptorRegistry getInterceptorRegistry();

    Extractors getExtractors();
}
