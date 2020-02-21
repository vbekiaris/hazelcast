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

package com.hazelcast.internal.serialization;

import com.hazelcast.core.ManagedContext;
import com.hazelcast.partition.PartitioningStrategy;

/**
 * SPI to serialize user objects to {@link Data} and back to Object
 * {@link Data} is the internal representation of binary data in hazelcast.
 */
public interface SerializationService {

    int ROOT_CONTEXT_ID = 0;

    /**
     * Serializes an object to a {@link Data}.
     * <p>
     * This method can safely be called with a {@link Data} instance. In that case, that instance is returned.
     * <p>
     * If this method is called with null, null is returned.
     *
     * @param obj the object to serialize.
     * @return the serialized object.
     * @throws com.hazelcast.nio.serialization.HazelcastSerializationException when serialization fails.
     */
    default <B extends Data> B toData(Object obj) {
        return toData(ROOT_CONTEXT_ID, obj);
    }

    <B extends Data> B toData(int contextId, Object obj);

    /**
     * Serializes an object to a {@link Data}.
     * <p>
     * This method can safely be called with a {@link Data} instance. In that case, that instance is returned.
     * <p>
     * If this method is called with null, null is returned.
     *
     * @param obj      the object to serialize.
     * @param strategy strategy is used to calculate partition ID of the resulting data see {@link PartitioningStrategy}
     * @return the serialized object.
     * @throws com.hazelcast.nio.serialization.HazelcastSerializationException when serialization fails.
     */
    default <B extends Data> B toData(Object obj, PartitioningStrategy strategy) {
        return toData(ROOT_CONTEXT_ID, obj, strategy);
    }

    <B extends Data> B toData(int contextId, Object obj, PartitioningStrategy strategy);

    /**
     * Deserializes an object.
     * <p>
     * This method can safely be called on an object that is already deserialized. In that case, that instance
     * is returned.
     * <p>
     * If this method is called with null, null is returned.
     *
     * @param data the data to deserialize.
     * @return the deserialized object.
     * @throws com.hazelcast.nio.serialization.HazelcastSerializationException when deserialization fails.
     */
    default <T> T toObject(Object data) {
        return toObject(ROOT_CONTEXT_ID, data);
    }

    <T> T toObject(int contextId, Object data);

    /**
     * Deserializes an object.
     * <p>
     * This method can safely be called on an object that is already deserialized. In that case, that instance
     * is returned.
     * <p>
     * If this method is called with null, null is returned.
     *
     * @param data  the data to deserialize.
     * @param klass The class to instantiate when deserializing the object.
     * @return the deserialized object.
     * @throws com.hazelcast.nio.serialization.HazelcastSerializationException when deserialization fails.
     */
    default <T> T toObject(Object data, Class klass) {
        return toObject(ROOT_CONTEXT_ID, data, klass);
    }

    <T> T toObject(int contextId, Object data, Class klass);

    /**
     * see {@link com.hazelcast.config.Config#setManagedContext(ManagedContext)}
     *
     * @return ManagedContext that is set by user in Config
     */
    ManagedContext getManagedContext();
}
