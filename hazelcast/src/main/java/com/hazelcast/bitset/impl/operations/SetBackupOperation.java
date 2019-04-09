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
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.BackupOperation;

import java.io.IOException;

public class SetBackupOperation extends AbstractBitSetOperation implements BackupOperation {

    private int bitIndex;
    private boolean set;

    public SetBackupOperation() {
    }

    public SetBackupOperation(String name, int bitIndex, boolean set) {
        super(name);
        this.bitIndex = bitIndex;
        this.set = set;
    }

    @Override
    public void run()
            throws Exception {
        if (set) {
            getContainer().set(bitIndex);
        } else {
            getContainer().clear(bitIndex);
        }
    }

    @Override
    public int getId() {
        return BitSetDataSerializerHook.SET_BACKUP_OPERATION;
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        this.bitIndex = in.readInt();
        this.set = in.readBoolean();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        out.writeInt(bitIndex);
        out.writeBoolean(set);
    }
}
