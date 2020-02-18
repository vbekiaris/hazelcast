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

package com.hazelcast.internal.serialization.impl.operation;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.SerializationDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;

import static com.hazelcast.internal.serialization.impl.SerializationDataSerializerHook.ADD_SERIALIZER_OPERATION;

public class AddSerializerOperation extends Operation implements IdentifiedDataSerializable {

    private String typeName;
    private String serializerClassName;

    public AddSerializerOperation() {
    }

    public AddSerializerOperation(String typeName, String serializerClassName) {
        this.typeName = typeName;
        this.serializerClassName = serializerClassName;
    }

    @Override
    public void run()
            throws Exception {
        InternalSerializationService serializationService =
                (InternalSerializationService) getNodeEngine().getSerializationService();
        serializationService.registerSerializer(typeName, serializerClassName);
    }

    @Override
    public int getFactoryId() {
        return SerializationDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ADD_SERIALIZER_OPERATION;
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        typeName = in.readUTF();
        serializerClassName = in.readUTF();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        out.writeUTF(typeName);
        out.writeUTF(serializerClassName);
    }
}
