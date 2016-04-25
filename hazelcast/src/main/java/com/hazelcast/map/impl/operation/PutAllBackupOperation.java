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

import com.hazelcast.core.EntryView;
import com.hazelcast.map.impl.EntryViews;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.event.MapEventPublisher;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.RecordInfo;
import com.hazelcast.map.impl.record.Records;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.impl.MutatingOperation;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PutAllBackupOperation extends MapOperation implements PartitionAwareOperation, BackupOperation,
        MutatingOperation {

    private List<Map.Entry<Data, Data>> entries;
    private List<RecordInfo> recordInfos;
    private RecordStore recordStore;

    public PutAllBackupOperation(String name, List<Map.Entry<Data, Data>> entries, List<RecordInfo> recordInfos) {
        super(name);
        this.entries = entries;
        this.recordInfos = recordInfos;
    }

    public PutAllBackupOperation() {
    }

    @Override
    public void run() {
        long now = Clock.currentTimeMillis();
        int partitionId = getPartitionId();
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        MapEventPublisher eventPublisher = mapServiceContext.getMapEventPublisher();
        recordStore = mapServiceContext.getRecordStore(partitionId, name);
        boolean wanEnabled = IMapContainer.isWanReplicationEnabled();
        for (int i = 0; i < entries.size(); i++) {
            final RecordInfo recordInfo = recordInfos.get(i);
            final Map.Entry<Data, Data> entry = entries.get(i);
            final Record record = recordStore.putBackup(entry.getKey(), entry.getValue());
            Records.applyRecordInfo(record, recordInfo);
            if (wanEnabled) {
                final Data dataValueAsData = mapServiceContext.toData(entry.getValue());
                final EntryView entryView = EntryViews.createSimpleEntryView(entry.getKey(), dataValueAsData, record);
                eventPublisher.publishWanReplicationUpdateBackup(name, entryView);
            }

            recordStore.evictEntries(now);
        }
    }

    @Override
    public Object getResponse() {
        return entries;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        final int size = entries.size();
        out.writeInt(size);
        for (int i = 0; i < size; i++) {
            final Map.Entry<Data, Data> entry = entries.get(i);
            out.writeData(entry.getKey());
            out.writeData(entry.getValue());
            recordInfos.get(i).writeData(out);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        final int size = in.readInt();
        entries = new ArrayList<Map.Entry<Data, Data>>(size);
        recordInfos = new ArrayList<RecordInfo>(size);
        for (int i = 0; i < size; i++) {
            Data key = in.readData();
            Data value = in.readData();
            Map.Entry entry = new AbstractMap.SimpleImmutableEntry<Data, Data>(key, value);
            entries.add(entry);
            final RecordInfo recordInfo = new RecordInfo();
            recordInfo.readData(in);
            recordInfos.add(recordInfo);
        }
    }
}
