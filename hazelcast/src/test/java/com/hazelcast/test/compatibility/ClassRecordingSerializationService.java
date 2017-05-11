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
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.PortableContext;
import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.nio.BufferObjectDataOutput;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.test.TestEnvironment;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteOrder;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListSet;

import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static com.hazelcast.util.StringUtil.LINE_SEPARATOR;
import static java.lang.String.format;

/**
 * Serialization service that records class names for which serialization/deserialization is performed.
 *
 * Employed to gather classes used in member-to-member communication during test suite execution with mock network.
 *
 * @see TestEnvironment#RECORD_SERIALIZED_CLASS_NAMES
 */
public class ClassRecordingSerializationService implements InternalSerializationService {

    public static final String FILE_NAME = TestEnvironment.getSerializedClassNamesPath() + randomString();

    private static final SortedSet<String> CLASS_NAMES = new ConcurrentSkipListSet<String>();

    private final InternalSerializationService delegate;

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(new ClassNamesPersister()));
    }

    public ClassRecordingSerializationService(InternalSerializationService delegate) {
        this.delegate = delegate;
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
        return delegate.toData(obj);
    }

    @Override
    public <B extends Data> B toData(Object obj, PartitioningStrategy strategy) {
        recordClassForObject(obj);
        return delegate.toData(obj, strategy);
    }

    @Override
    public void writeObject(ObjectDataOutput out, Object obj) {
        recordClassForObject(obj);
        delegate.writeObject(out, obj);
    }

    @Override
    public <T> T readObject(ObjectDataInput in) {
        return (T) recordClassForObject(delegate.readObject(in));
    }

    @Override
    public <T> T readObject(ObjectDataInput in, Class aClass) {
        recordClass(aClass);
        return (T) recordClassForObject(delegate.readObject(in, aClass));
    }

    @Override
    public BufferObjectDataInput createObjectDataInput(byte[] data) {
        return delegate.createObjectDataInput(data);
    }

    @Override
    public BufferObjectDataInput createObjectDataInput(Data data) {
        return delegate.createObjectDataInput(data);
    }

    @Override
    public BufferObjectDataOutput createObjectDataOutput(int size) {
        return delegate.createObjectDataOutput(size);
    }

    @Override
    public <T> T toObject(Object data) {
        return (T) recordClassForObject(delegate.toObject(data));
    }

    @Override
    public ClassLoader getClassLoader() {
        return delegate.getClassLoader();
    }

    @Override
    public <T> T toObject(Object data, Class klazz) {
        recordClass(klazz);
        return (T) recordClassForObject(delegate.toObject(data, klazz));
    }

    @Override
    public ManagedContext getManagedContext() {
        return delegate.getManagedContext();
    }

    @Override
    public byte[] toBytes(Object obj) {
        recordClassForObject(obj);
        return delegate.toBytes(obj);
    }

    @Override
    public byte[] toBytes(Object obj, int leftPadding, boolean insertPartitionHash) {
        recordClassForObject(obj);
        return delegate.toBytes(obj, leftPadding, insertPartitionHash);
    }

    @Override
    public PortableReader createPortableReader(Data data)
            throws IOException {
        return delegate.createPortableReader(data);
    }

    @Override
    public PortableContext getPortableContext() {
        return delegate.getPortableContext();
    }

    @Override
    public void disposeData(Data data) {
        delegate.disposeData(data);
    }

    @Override
    public BufferObjectDataOutput createObjectDataOutput() {
        return delegate.createObjectDataOutput();
    }

    @Override
    public ByteOrder getByteOrder() {
        return delegate.getByteOrder();
    }

    @Override
    public byte getVersion() {
        return delegate.getVersion();
    }

    @Override
    public void dispose() {
        delegate.dispose();
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
