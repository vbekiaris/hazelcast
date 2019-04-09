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

import com.hazelcast.bitset.impl.operations.GetOperation;
import com.hazelcast.bitset.impl.operations.SetOperation;
import com.hazelcast.core.IBitSet;
import com.hazelcast.cp.internal.datastructures.atomicref.operation.SetOp;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.NodeEngine;

public class BitSetProxy extends AbstractDistributedObject<BitSetService> implements IBitSet {

    private final String name;
    private final int partitionId;

    public BitSetProxy(NodeEngine nodeEngine, BitSetService service, String name) {
        super(nodeEngine, service);
        this.name = name;
        this.partitionId = nodeEngine.getPartitionService().getPartitionId(getNameAsPartitionAwareData());
    }

    @Override
    public boolean get(int bitIndex) {
        GetOperation getOperation = new GetOperation(name, bitIndex);
        getOperation.setPartitionId(partitionId);
        Boolean returnValue = (Boolean) getOperationService().invokeOnPartition(getOperation).join();
        return returnValue;
    }

    @Override
    public void set(int bitIndex) {
        SetOperation setOperation = new SetOperation(name, bitIndex, true);
        setOperation.setPartitionId(partitionId);
        getOperationService().invokeOnPartition(setOperation).join();
    }

    @Override
    public void clear(int bitIndex) {
        SetOperation setOperation = new SetOperation(name, bitIndex, false);
        setOperation.setPartitionId(partitionId);
        getOperationService().invokeOnPartition(setOperation).join();
    }

    @Override
    public void clear() {

    }

    @Override
    public int cardinality() {
        return 0;
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public String getServiceName() {
        return BitSetService.SERVICE_NAME;
    }

    @Override
    public String getName() {
        return name;
    }


}
