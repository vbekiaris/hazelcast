/*
 * Copyright (c) 2008, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.test.clusterprotocol;

import com.hazelcast.core.ManagedContext;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.PortableContext;
import com.hazelcast.internal.serialization.impl.SerializationServiceV1;
import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.nio.BufferObjectDataOutput;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Wraps {@link SerializationServiceV1} and records the types of objects being serialized and deserialized
 * in a set.
 */
public class ClassInterceptingSerializationService implements InternalSerializationService {

    private final InternalSerializationService serializationServiceV1;

    private static final Set<String> protocolClassNames = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                System.out.println("Intercepted cluster protocol classes: ");
                for (String s : protocolClassNames) {
                    System.out.println(s);
                }
            }
        }));
    }

    public ClassInterceptingSerializationService(InternalSerializationService serializationServiceV1) {
        this.serializationServiceV1 = serializationServiceV1;
    }

    private void recordClassName(Object obj) {
        if (obj == null) {
            return;
        }
        Class klass = obj.getClass();
        if (klass.isPrimitive()) {
            return;
        }
        if (klass.isArray()) {
            if (klass.getComponentType().isPrimitive()) {
                return;
            } else {
                protocolClassNames.add(klass.getComponentType().getName());
            }
        } else {
            protocolClassNames.add(klass.getName());
        }
    }

    @Override
    public <B extends Data> B toData(Object obj) {
        recordClassName(obj);
        return serializationServiceV1.toData(obj);
    }

    @Override
    public void writeObject(ObjectDataOutput out, Object obj) {
        recordClassName(obj);
        serializationServiceV1.writeObject(out, obj);
    }

    @Override
    public <T> T readObject(ObjectDataInput in) {
        T obj = serializationServiceV1.readObject(in);
        recordClassName(obj);
        return obj;
    }

    @Override
    public <T> T readObject(ObjectDataInput in, Class aClass) {
        T obj = serializationServiceV1.readObject(in, aClass);
        recordClassName(obj);
        return obj;
    }

    @Override
    public BufferObjectDataInput createObjectDataInput(byte[] data) {
        return serializationServiceV1.createObjectDataInput(data);
    }

    @Override
    public BufferObjectDataInput createObjectDataInput(Data data) {
        return serializationServiceV1.createObjectDataInput(data);
    }

    @Override
    public BufferObjectDataOutput createObjectDataOutput(int size) {
        return serializationServiceV1.createObjectDataOutput(size);
    }

    @Override
    public ClassLoader getClassLoader() {
        return serializationServiceV1.getClassLoader();
    }

    @Override
    public <B extends Data> B toData(Object obj, PartitioningStrategy strategy) {
        recordClassName(obj);
        return serializationServiceV1.toData(obj, strategy);
    }

    @Override
    public <T> T toObject(Object data) {
        T obj = serializationServiceV1.toObject(data);
        recordClassName(obj);
        return obj;
    }

    @Override
    public <T> T toObject(Object data, Class klazz) {
        T obj = serializationServiceV1.toObject(data, klazz);
        recordClassName(obj);
        return obj;
    }

    @Override
    public ManagedContext getManagedContext() {
        return serializationServiceV1.getManagedContext();
    }

    @Override
    public byte[] toBytes(Object obj) {
        recordClassName(obj);
        return serializationServiceV1.toBytes(obj);
    }

    @Override
    public byte[] toBytes(int padding, Object obj) {
        recordClassName(obj);
        return serializationServiceV1.toBytes(padding, obj);
    }

    @Override
    public byte[] toBytes(Object obj, PartitioningStrategy strategy) {
        recordClassName(obj);
        return serializationServiceV1.toBytes(obj, strategy);
    }

    @Override
    public byte[] toBytes(int padding, Object obj, PartitioningStrategy strategy) {
        return serializationServiceV1.toBytes(padding, obj, strategy);
    }

    @Override
    public PortableReader createPortableReader(Data data)
            throws IOException {
        return serializationServiceV1.createPortableReader(data);
    }

    @Override
    public PortableContext getPortableContext() {
        return serializationServiceV1.getPortableContext();
    }

    @Override
    public void disposeData(Data data) {
        serializationServiceV1.disposeData(data);
    }

    @Override
    public BufferObjectDataOutput createObjectDataOutput() {
        return serializationServiceV1.createObjectDataOutput();
    }

    @Override
    public ByteOrder getByteOrder() {
        return serializationServiceV1.getByteOrder();
    }

    @Override
    public byte getVersion() {
        return serializationServiceV1.getVersion();
    }

    @Override
    public void dispose() {
        serializationServiceV1.dispose();
    }

}
