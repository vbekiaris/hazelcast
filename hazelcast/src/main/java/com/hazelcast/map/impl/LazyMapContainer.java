package com.hazelcast.map.impl;

import com.hazelcast.config.MapConfig;
import com.hazelcast.core.IFunction;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.internal.serialization.SerializationService;
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
 * A class that is light to construct and can be used as a placeholder in a {@code ConcurrentHashMap}
 * that will delegate to
 */
public class LazyMapContainer implements IMapContainer {

    private volatile MapContainer container;

    private final ConstructorFunction<String, MapContainer> constructorFunction;
    private final String mapName;

    public LazyMapContainer(String name, ConstructorFunction<String, MapContainer> constructorFunction) {
        this.constructorFunction = constructorFunction;
        this.mapName = name;
    }

    private MapContainer getContainer() {
        MapContainer mapContainer = container;
        if (mapContainer == null) {
            synchronized (this) {
                mapContainer = container;
                if (mapContainer == null) {
                    mapContainer = constructorFunction.createNew(mapName);
                    container = mapContainer;
                }
            }
        }
        return mapContainer;
    }

    public Evictor createEvictor(MapServiceContext mapServiceContext) {
        return getContainer().createEvictor(mapServiceContext);
    }

    public ConstructorFunction<Void, RecordFactory> createRecordFactoryConstructor(SerializationService serializationService) {
        return getContainer().createRecordFactoryConstructor(serializationService);
    }

    @Override
    public void initWanReplication(NodeEngine nodeEngine) {
        getContainer().initWanReplication(nodeEngine);
    }

    @Override
    public Indexes getIndexes() {
        return getContainer().getIndexes();
    }

    @Override
    public WanReplicationPublisher getWanReplicationPublisher() {
        return getContainer().getWanReplicationPublisher();
    }

    @Override
    public MapMergePolicy getWanMergePolicy() {
        return getContainer().getWanMergePolicy();
    }

    @Override
    public boolean isWanReplicationEnabled() {
        return getContainer().isWanReplicationEnabled();
    }

    @Override
    public void checkWanReplicationQueues() {
        getContainer().checkWanReplicationQueues();
    }

    @Override
    public boolean hasMemberNearCache() {
        return getContainer().hasMemberNearCache();
    }

    @Override
    public int getTotalBackupCount() {
        return getContainer().getTotalBackupCount();
    }

    @Override
    public int getBackupCount() {
        return getContainer().getBackupCount();
    }

    @Override
    public int getAsyncBackupCount() {
        return getContainer().getAsyncBackupCount();
    }

    @Override
    public PartitioningStrategy getPartitioningStrategy() {
        return getContainer().getPartitioningStrategy();
    }

    @Override
    public SizeEstimator getNearCacheSizeEstimator() {
        return getContainer().getNearCacheSizeEstimator();
    }

    @Override
    public MapServiceContext getMapServiceContext() {
        return getContainer().getMapServiceContext();
    }

    @Override
    public MapStoreContext getMapStoreContext() {
        return getContainer().getMapStoreContext();
    }

    @Override
    public MapConfig getMapConfig() {
        return getContainer().getMapConfig();
    }

    @Override
    public void setMapConfig(MapConfig mapConfig) {
        getContainer().setMapConfig(mapConfig);
    }

    @Override
    public String getName() {
        return getContainer().getName();
    }

    @Override
    public String getQuorumName() {
        return getContainer().getQuorumName();
    }

    @Override
    public IFunction<Object, Data> toData() {
        return getContainer().toData();
    }

    @Override
    public ConstructorFunction<Void, RecordFactory> getRecordFactoryConstructor() {
        return getContainer().getRecordFactoryConstructor();
    }

    @Override
    public QueryableEntry newQueryEntry(Data key, Object value) {
        return getContainer().newQueryEntry(key, value);
    }

    @Override
    public Evictor getEvictor() {
        return getContainer().getEvictor();
    }

    @Override
    public void setEvictor(Evictor evictor) {
        getContainer().setEvictor(evictor);
    }

    @Override
    public Extractors getExtractors() {
        return getContainer().getExtractors();
    }

    @Override
    public boolean isMemberNearCacheInvalidationEnabled() {
        return getContainer().isMemberNearCacheInvalidationEnabled();
    }

    @Override
    public boolean hasInvalidationListener() {
        return getContainer().hasInvalidationListener();
    }

    @Override
    public void increaseInvalidationListenerCount() {
        getContainer().increaseInvalidationListenerCount();
    }

    @Override
    public void decreaseInvalidationListenerCount() {
        getContainer().decreaseInvalidationListenerCount();
    }

    @Override
    public boolean isInvalidationEnabled() {
        return getContainer().isInvalidationEnabled();
    }

    @Override
    public InterceptorRegistry getInterceptorRegistry() {
        return getContainer().getInterceptorRegistry();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        LazyMapContainer that = (LazyMapContainer) o;

        return mapName.equals(that.mapName);

    }

    @Override
    public int hashCode() {
        return mapName.hashCode();
    }
}
