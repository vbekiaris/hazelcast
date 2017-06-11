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

package com.hazelcast.client.impl.protocol.task.dynamicconfig;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.DynamicConfigAddCardinalityEstimatorConfigCodec;
import com.hazelcast.config.CardinalityEstimatorConfig;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.dynamicconfig.AddDynamicConfigOperationFactory;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.OperationFactory;

public class AddCardinalityEstimatorConfigMessageTask
        extends AbstractAddConfigMessageTask<DynamicConfigAddCardinalityEstimatorConfigCodec.RequestParameters> {

    public AddCardinalityEstimatorConfigMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected DynamicConfigAddCardinalityEstimatorConfigCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return DynamicConfigAddCardinalityEstimatorConfigCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return DynamicConfigAddCardinalityEstimatorConfigCodec.encodeResponse();
    }

    @Override
    protected OperationFactory getOperationFactory() {
        CardinalityEstimatorConfig config = new CardinalityEstimatorConfig(parameters.name, parameters.backupCount,
                parameters.asyncBackupCount);
        return new AddDynamicConfigOperationFactory(config);
    }
}
