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
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.NamedOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;

import java.io.IOException;

public abstract class AbstractBitSetOperation extends Operation
        implements NamedOperation, PartitionAwareOperation, IdentifiedDataSerializable {

    protected String name;

    public AbstractBitSetOperation() {
    }

    public AbstractBitSetOperation(String name) {
        this.name = name;
    }

    @Override
    public int getFactoryId() {
        return BitSetDataSerializerHook.F_ID;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        name = in.readUTF();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        out.writeUTF(name);
    }

    @Override
    public String getServiceName() {
        return BitSetService.SERVICE_NAME;
    }

    protected BitSetContainer getContainer() {
        BitSetService service = getService();
        return service.getContainer(name);
    }


}
