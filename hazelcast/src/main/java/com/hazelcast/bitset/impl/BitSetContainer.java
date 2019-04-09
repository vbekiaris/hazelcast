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

package com.hazelcast.bitset.impl;

import com.hazelcast.bitset.BitSetDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.BitSet;

public class BitSetContainer implements IdentifiedDataSerializable {

    private BitSet bitSet;

    public BitSetContainer() {
        this.bitSet = new BitSet();
    }

    public boolean get(int bitIndex) {
        return bitSet.get(bitIndex);
    }

    public void set(int bitIndex) {
        bitSet.set(bitIndex);
    }

    public void clear(int bitIndex) {
        bitSet.clear(bitIndex);
    }

    public void clear() {
        bitSet.clear();
    }

    public BitSet getBitSet() {
        return bitSet;
    }

    public void or(BitSetContainer set) {
        bitSet.or(set.bitSet);
    }

    @Override
    public int getFactoryId() {
        return BitSetDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return BitSetDataSerializerHook.BITSET_CONTAINER;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        out.writeObject(bitSet);
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        bitSet = in.readObject();
    }
}
