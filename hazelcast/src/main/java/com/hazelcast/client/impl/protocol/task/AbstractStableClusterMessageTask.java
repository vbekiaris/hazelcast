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

package com.hazelcast.client.impl.protocol.task;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static com.hazelcast.internal.util.InvocationUtil.invokeOnStableClusterSerial;

public abstract class AbstractStableClusterMessageTask<P> extends AbstractMessageTask<P>
        implements BiConsumer<Object, Throwable> {

    private static final int RETRY_COUNT = 100;

    protected AbstractStableClusterMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected void processMessage() throws Throwable {
        InternalCompletableFuture<Object> future =
                invokeOnStableClusterSerial(nodeEngine, createOperationSupplier(), RETRY_COUNT);
        future.whenCompleteAsync(this);
    }

    abstract Supplier<Operation> createOperationSupplier();

    protected abstract Object resolve(Object response);

    @Override
    public void accept(Object response, Throwable throwable) {
        if (throwable == null) {
            sendResponse(resolve(response));
        } else {
            handleProcessingFailure(throwable);
        }
    }
}
