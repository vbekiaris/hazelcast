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

package com.hazelcast.bitset.impl.operations;

import com.hazelcast.bitset.BitSetDataSerializerHook;
import com.hazelcast.bitset.BitSetService;
import com.hazelcast.bitset.impl.BitSetContainer;
import com.hazelcast.internal.serialization.impl.SerializationUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;

import java.io.IOException;
import java.util.BitSet;
import java.util.Map;

public class ReplicationOperation extends Operation
        implements PartitionAwareOperation, IdentifiedDataSerializable {

    private Map<String, BitSetContainer> migrationData;

    public ReplicationOperation() {
    }

    public ReplicationOperation(Map<String, BitSetContainer> migrationData) {
        this.migrationData = migrationData;
    }

    @Override
    public int getFactoryId() {
        return BitSetDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return BitSetDataSerializerHook.REPLICATION_OPERATION;
    }

    @Override
    public String getServiceName() {
        return BitSetService.SERVICE_NAME;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        SerializationUtil.writeMap(migrationData, out);
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        this.migrationData = SerializationUtil.readMap(in);
    }

    @Override
    public void run()
            throws Exception {
        BitSetService service = getService();
        service.addContainers(migrationData);
    }
}
