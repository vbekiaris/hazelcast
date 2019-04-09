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
import com.hazelcast.bitset.impl.operations.AndBackupOperation;
import com.hazelcast.bitset.impl.operations.AndOperation;
import com.hazelcast.bitset.impl.operations.CardinalityOperation;
import com.hazelcast.bitset.impl.operations.ClearOperation;
import com.hazelcast.bitset.impl.operations.GetContainerOperation;
import com.hazelcast.bitset.impl.operations.GetOperation;
import com.hazelcast.bitset.impl.operations.OrBackupOperation;
import com.hazelcast.bitset.impl.operations.OrOperation;
import com.hazelcast.bitset.impl.operations.ReplicationOperation;
import com.hazelcast.bitset.impl.operations.SetBackupOperation;
import com.hazelcast.bitset.impl.operations.SetOperation;
import com.hazelcast.bitset.impl.operations.SizeOperation;
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
    public static final int CLEAR_OPERATION = 10;
    public static final int CARDINALITY_OPERATION = 11;
    public static final int SIZE_OPERATION = 12;
    public static final int CLEAR_BACKUP_OPERATION = 13;

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
                    case CLEAR_OPERATION:
                        return new ClearOperation();
                    case SIZE_OPERATION:
                        return new SizeOperation();
                    case CARDINALITY_OPERATION:
                        return new CardinalityOperation();
                    default:
                        return null;
                }
            }
        };
    }
}
