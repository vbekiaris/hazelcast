/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.test.compatibility;

import com.hazelcast.core.ManagedContext;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.internal.serialization.InputOutputFactory;
import com.hazelcast.internal.serialization.impl.SerializationServiceV1;
import com.hazelcast.internal.serialization.impl.bufferpool.BufferPoolFactory;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.PortableFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListSet;

import static com.hazelcast.util.StringUtil.LINE_SEPARATOR;
import static java.lang.String.format;
import static java.util.UUID.randomUUID;

/**
 * Serialization service that records class names for which serialization/deserialization is performed.
 *
 * Employed to gather classes used in member-to-member communication during test suite execution with mock network.
 *
 */
public class ClassRecordingSerializationService extends SerializationServiceV1 {

    public static final String RECORD_SERIALIZED_CLASS_NAMES = "hazelcast.test.record.serialized.class.names";

    public static final String FILE_NAME = System.getProperty(RECORD_SERIALIZED_CLASS_NAMES) + randomUUID().toString();

    private static final SortedSet<String> CLASS_NAMES = new ConcurrentSkipListSet<String>();

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(new ClassNamesPersister()));
    }

    public ClassRecordingSerializationService(InputOutputFactory inputOutputFactory, byte version, int portableVersion, ClassLoader classLoader,
                                              Map<Integer, ? extends DataSerializableFactory> dataSerializableFactories,
                                              Map<Integer, ? extends PortableFactory> portableFactories, ManagedContext managedContext,
                                              PartitioningStrategy globalPartitionStrategy, int initialOutputBufferSize, BufferPoolFactory bufferPoolFactory,
                                              boolean enableCompression, boolean enableSharedObject) {
        super(inputOutputFactory, version, portableVersion, classLoader, dataSerializableFactories,
                portableFactories, managedContext, globalPartitionStrategy, initialOutputBufferSize, bufferPoolFactory,
                enableCompression, enableSharedObject);
    }

    // record the class of given object, then return it
    private static <T> T recordClassForObject(T obj) {
        if (obj == null)
            return null;

        Class<?> klass = obj.getClass();
        recordClass(klass);
        return obj;
    }

    private static void recordClass(Class<?> klass) {
        if (klass.isArray()) {
            klass = klass.getComponentType();
        }

        if (klass.isPrimitive())
            return;

        CLASS_NAMES.add(klass.getName());
    }

    @Override
    public <B extends Data> B toData(Object obj) {
        recordClassForObject(obj);
        return super.toData(obj);
    }

    @Override
    public <B extends Data> B toData(Object obj, PartitioningStrategy strategy) {
        recordClassForObject(obj);
        return super.toData(obj, strategy);
    }

    @Override
    public void writeObject(ObjectDataOutput out, Object obj) {
        recordClassForObject(obj);
        super.writeObject(out, obj);
    }

    @Override
    public <T> T readObject(ObjectDataInput in) {
        return (T) recordClassForObject(super.readObject(in));
    }

    @Override
    public <T> T readObject(ObjectDataInput in, Class aClass) {
        recordClass(aClass);
        return (T) recordClassForObject(super.readObject(in, aClass));
    }

    @Override
    public <T> T toObject(Object data) {
        return (T) recordClassForObject(super.toObject(data));
    }

    @Override
    public <T> T toObject(Object data, Class klazz) {
        recordClass(klazz);
        return (T) recordClassForObject(super.toObject(data, klazz));
    }

    @Override
    public byte[] toBytes(Object obj) {
        recordClassForObject(obj);
        return super.toBytes(obj);
    }

    @Override
    public byte[] toBytes(int padding, Object obj) {
        recordClassForObject(obj);
        return super.toBytes(padding, obj);
    }

    @Override
    public byte[] toBytes(Object obj, PartitioningStrategy strategy) {
        recordClassForObject(obj);
        return super.toBytes(obj, strategy);
    }

    @Override
    public byte[] toBytes(int padding, Object obj, PartitioningStrategy strategy) {
        recordClassForObject(obj);
        return super.toBytes(padding, obj, strategy);
    }

    public static class ClassNamesPersister implements Runnable {
        @Override
        public void run() {
            System.out.println(format("Persisting %d recorded serialized class names to %s",
                    CLASS_NAMES.size(), FILE_NAME));

            FileWriter fileWriter = null;
            try {
                fileWriter = new FileWriter(FILE_NAME);
                for (String className : CLASS_NAMES) {
                    fileWriter.write(className + LINE_SEPARATOR);
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if (fileWriter != null) {
                    try {
                        fileWriter.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
            System.out.println(format("Recorded class names were persisted to %s", FILE_NAME));
        }
    }
}
