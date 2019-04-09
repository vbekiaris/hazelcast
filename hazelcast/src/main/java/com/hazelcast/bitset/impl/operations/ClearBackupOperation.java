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
import com.hazelcast.spi.BackupOperation;

public class ClearBackupOperation extends AbstractBitSetOperation implements BackupOperation {

    public ClearBackupOperation() {
    }

    public ClearBackupOperation(String name) {
        super(name);
    }

    @Override
    public int getId() {
        return BitSetDataSerializerHook.CLEAR_BACKUP_OPERATION;
    }

    @Override
    public void run()
            throws Exception {
        getContainer().clear();
    }
}
