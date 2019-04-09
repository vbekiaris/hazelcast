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

package com.hazelcast.bitset;

import com.hazelcast.bitset.impl.BitSetContainer;
import com.hazelcast.bitset.impl.operations.*;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

public class BitSetDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.BITSET_DS_FACTORY,
            FactoryIdHelper.BITSET_DS_FACTORY_ID);

    public static final int GET_OPERATION = 0;
    public static final int SET_OPERATION = 1;
    public static final int SET_BACKUP_OPERATION = 2;
    public static final int REPLICATION_OPERATION = 3;
    public static final int BITSET_CONTAINER = 4;
    public static final int AND_OPERATION = 5;
    public static final int AND_BACKUP_OPERATION = 6;
    public static final int OR_OPERATION = 7;
    public static final int OR_BACKUP_OPERATION = 8;
    public static final int GET_CONTAINER_OPERATION = 9;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return new DataSerializableFactory() {
            @Override
            public IdentifiedDataSerializable create(int typeId) {
                switch (typeId) {
                    case GET_OPERATION:
                        return new GetOperation();
                    case SET_OPERATION:
                        return new SetOperation();
                    case SET_BACKUP_OPERATION:
                        return new SetBackupOperation();
                    case REPLICATION_OPERATION:
                        return new ReplicationOperation();
                    case BITSET_CONTAINER:
                        return new BitSetContainer();
                    case AND_OPERATION:
                        return new AndOperation();
                    case AND_BACKUP_OPERATION:
                        return new AndBackupOperation();
                    case OR_OPERATION:
                        return new OrOperation();
                    case OR_BACKUP_OPERATION:
                        return new OrBackupOperation();
                    case GET_CONTAINER_OPERATION:
                        return new GetContainerOperation();
                    default:
                        return null;
                }
            }
        };
    }
}
