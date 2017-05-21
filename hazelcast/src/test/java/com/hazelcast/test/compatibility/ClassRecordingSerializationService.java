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

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;

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
    public static final String SAMPLES_FILE_SUFFIX = ".samples";
    public static final String INDEX_FILE_SUFFIX = ".index";

    private static final int MAX_SERIALIZED_SAMPLES_PER_CLASS = 3;
    private static final SortedSet<String> CLASS_NAMES = new ConcurrentSkipListSet<String>();
    private static final ConcurrentMap<String, List<byte[]>> SERIALIZED_SAMPLES_PER_CLASS_NAME =
            new ConcurrentHashMap<String, List<byte[]>>(1000);

    private final InternalSerializationService delegate;

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(new ClassNamesPersister()));
    }

    public ClassRecordingSerializationService(InternalSerializationService delegate) {
        this.delegate = delegate;
    }

    private static <T> T recordClassForObject(T obj) {
        return recordClassForObject(obj, null);
    }

    // record the class of given object, then return it
    private static <T> T recordClassForObject(T obj, byte[] serializedObject) {
        if (obj == null)
            return null;

        Class<?> klass = obj.getClass();
        recordClass(klass);
        if (serializedObject != null && shouldAddSerializedSample(obj)) {
            addSerializedSample(obj, serializedObject);
        }
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
        B data = delegate.toData(obj);
        recordClassForObject(obj, data == null ? null : data.toByteArray());
        return data;
    }

    @Override
    public <B extends Data> B toData(Object obj, PartitioningStrategy strategy) {
        B data = delegate.toData(obj, strategy);
        recordClassForObject(obj, data == null ? null : data.toByteArray());
        return data;
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
    public <T> T toObject(Object data) {
        return (T) recordClassForObject(delegate.toObject(data));
    }

    @Override
    public <T> T toObject(Object data, Class klazz) {
        recordClass(klazz);
        return (T) recordClassForObject(delegate.toObject(data, klazz));
    }

    @Override
    public byte[] toBytes(Object obj) {
        byte[] bytes = delegate.toBytes(obj);
        recordClassForObject(obj, bytes);
        return bytes;
    }

    @Override
    public byte[] toBytes(int padding, Object obj) {
        byte[] bytes = delegate.toBytes(padding, obj);
        recordClassForObject(obj, bytes);
        return bytes;
    }

    @Override
    public byte[] toBytes(Object obj, PartitioningStrategy strategy) {
        byte[] bytes = delegate.toBytes(obj, strategy);
        recordClassForObject(obj, bytes);
        return bytes;
    }

    @Override
    public byte[] toBytes(int padding, Object obj, PartitioningStrategy strategy) {
        byte[] bytes = delegate.toBytes(padding, obj, strategy);
        recordClassForObject(obj, bytes);
        return bytes;
    }

    @Override
    public ClassLoader getClassLoader() {
        return delegate.getClassLoader();
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
    public ManagedContext getManagedContext() {
        return delegate.getManagedContext();
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

    private static void addSerializedSample(Object obj, byte[] bytes) {
        String className = obj.getClass().getName();
        SERIALIZED_SAMPLES_PER_CLASS_NAME.putIfAbsent(className, new CopyOnWriteArrayList<byte[]>());
        List<byte[]> samples = SERIALIZED_SAMPLES_PER_CLASS_NAME.get(className);
        if (samples.size() < MAX_SERIALIZED_SAMPLES_PER_CLASS) {
            samples.add(bytes);
        }
    }

    private static boolean shouldAddSerializedSample(Object obj) {
        String className = obj.getClass().getName();
        List<byte[]> existingSamples = SERIALIZED_SAMPLES_PER_CLASS_NAME.get(className);
        if (existingSamples == null) {
            return true;
        }
        if (existingSamples.size() < MAX_SERIALIZED_SAMPLES_PER_CLASS) {
            return true;
        }
        return false;
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

            FileOutputStream serializedSamplesOutput = null;
            FileWriter indexOutput = null;
            try {
                serializedSamplesOutput = new FileOutputStream(FILE_NAME + SAMPLES_FILE_SUFFIX);
                FileChannel samplesOutputChannel = serializedSamplesOutput.getChannel();
                // index file format: className,startOfSample1,lengthOfSample1,startOfSample2,lengthOfSample2,...
                indexOutput = new FileWriter(FILE_NAME + INDEX_FILE_SUFFIX);

                for (Map.Entry<String, List<byte[]>> entry : SERIALIZED_SAMPLES_PER_CLASS_NAME.entrySet()) {
                    if (entry.getValue().isEmpty()) {
                        continue;
                    }
                    List<byte[]> samples = entry.getValue();
                    indexOutput.write(entry.getKey());
                    for (int i = 0; i < samples.size(); i++) {
                        byte[] sample = samples.get(i);
                        indexOutput.write("," + samplesOutputChannel.position() + "," + sample.length);
                        serializedSamplesOutput.write(sample);
                    }
                    indexOutput.write(LINE_SEPARATOR);
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if (indexOutput != null) {
                    try {
                        indexOutput.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                if (serializedSamplesOutput != null) {
                    try {
                        serializedSamplesOutput.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }

        }
    }
}
