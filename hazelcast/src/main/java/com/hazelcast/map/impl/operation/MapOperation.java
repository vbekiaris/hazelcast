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

package com.hazelcast.map.impl.operation;

import com.hazelcast.map.impl.IMapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.mapstore.MapDataStore;
import com.hazelcast.map.impl.nearcache.NearCacheProvider;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.AbstractNamedOperation;

import java.util.List;

import static com.hazelcast.util.CollectionUtil.isEmpty;

public abstract class MapOperation extends AbstractNamedOperation {

    protected transient MapService mapService;
    protected transient IMapContainer IMapContainer;
    protected transient MapServiceContext mapServiceContext;

    public MapOperation() {
    }

    public MapOperation(String name) {
        this.name = name;
    }

    // for testing only
    public void setMapService(MapService mapService) {
        this.mapService = mapService;
    }

    // for testing only
    public void setMapContainer(IMapContainer IMapContainer) {
        this.IMapContainer = IMapContainer;
    }

    @Override
    public void beforeRun() throws Exception {
        super.beforeRun();
        mapService = getService();
        mapServiceContext = mapService.getMapServiceContext();
        IMapContainer = mapServiceContext.getMapContainer(name);
        innerBeforeRun();
    }


    public void innerBeforeRun() throws Exception {
    }

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public void afterRun() throws Exception {
    }

    protected boolean isPostProcessing(RecordStore recordStore) {
        MapDataStore mapDataStore = recordStore.getMapDataStore();
        return mapDataStore.isPostProcessingMapStore() || mapServiceContext.hasInterceptor(name);
    }

    public void setThreadId(long threadId) {
        throw new UnsupportedOperationException();
    }

    public long getThreadId() {
        throw new UnsupportedOperationException();
    }

    protected final void invalidateNearCache(List<Data> keys) {
        if (!IMapContainer.isInvalidationEnabled() || isEmpty(keys)) {
            return;
        }
        NearCacheProvider nearCacheProvider = mapServiceContext.getNearCacheProvider();
        nearCacheProvider.getNearCacheInvalidator().invalidate(name, keys, getCallerUuid());
    }

    protected final void invalidateNearCache(Data key) {
        if (!IMapContainer.isInvalidationEnabled() || key == null) {
            return;
        }
        NearCacheProvider nearCacheProvider = mapServiceContext.getNearCacheProvider();
        nearCacheProvider.getNearCacheInvalidator().invalidate(name, key, getCallerUuid());
    }

    protected final void clearNearCache(boolean owner) {
        if (!IMapContainer.isInvalidationEnabled()) {
            return;
        }
        NearCacheProvider nearCacheProvider = mapServiceContext.getNearCacheProvider();
        nearCacheProvider.getNearCacheInvalidator().clear(name, owner, getCallerUuid());
    }
}
