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
import com.hazelcast.client.impl.protocol.task.AbstractCallableMessageTask;
import com.hazelcast.config.ItemListenerConfig;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.dynamicconfig.ClusterWideConfigurationService;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.util.InvocationUtil;

import java.security.Permission;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.internal.dynamicconfig.ClusterWideConfigurationService.CONFIG_PUBLISH_MAX_ATTEMPT_COUNT;

/**
 * Base implementation for dynamic add***Config methods.
 */
public abstract class AbstractAddConfigMessageTask<P> extends AbstractCallableMessageTask<P> {

    public AbstractAddConfigMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Object call() throws Exception {
        InvocationUtil.invokeOnStableCluster(nodeEngine, getOperationFactory(), null,
                CONFIG_PUBLISH_MAX_ATTEMPT_COUNT);
        return true;
    }

    @Override
    public String getServiceName() {
        return ClusterWideConfigurationService.SERVICE_NAME;
    }

    @Override
    public String getDistributedObjectName() {
        // todo need to specify for security
        return null;
    }

    @Override
    public String getMethodName() {
        // todo need to specify for security
        return null;
    }

    @Override
    public Permission getRequiredPermission() {
        // todo proper security for client-side config updates
        return null;
    }

    @Override
    public Object[] getParameters() {
        // todo may have to specify for security
        return new Object[0];
    }

    protected abstract OperationFactory getOperationFactory();

    protected List<ListenerConfig> adaptListenerConfigs(List<ListenerConfigHolder> listenerConfigHolders) {
        if (listenerConfigHolders == null || listenerConfigHolders.isEmpty()) {
            return null;
        }

        List<ListenerConfig> itemListenerConfigs = new ArrayList<ListenerConfig>();
        for (ListenerConfigHolder listenerConfigHolder : listenerConfigHolders) {
            itemListenerConfigs.add(listenerConfigHolder.asListenerConfig(serializationService));
        }
        return itemListenerConfigs;
    }

    protected List<ItemListenerConfig> adaptItemListenerConfigs(List<ItemListenerConfigHolder> configHolders) {
        if (configHolders == null || configHolders.isEmpty()) {
            return null;
        }

        List<ItemListenerConfig> itemListenerConfigs = new ArrayList<ItemListenerConfig>(configHolders.size());
        for (ItemListenerConfigHolder listenerConfigHolder : configHolders) {
            itemListenerConfigs.add(listenerConfigHolder.asItemListenerConfig(serializationService));
        }
        return itemListenerConfigs;
    }
}
