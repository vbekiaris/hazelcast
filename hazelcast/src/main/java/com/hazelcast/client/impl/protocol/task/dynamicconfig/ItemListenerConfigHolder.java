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

import com.hazelcast.config.ItemListenerConfig;
import com.hazelcast.core.ItemListener;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.serialization.SerializationService;

public class ItemListenerConfigHolder extends ListenerConfigHolder {

    private final boolean includeValue;

    public ItemListenerConfigHolder(String className, boolean includeValue) {
        super(className);
        this.includeValue = includeValue;
    }

    public ItemListenerConfigHolder(Data listenerImplementation, boolean includeValue) {
        super(listenerImplementation);
        this.includeValue = includeValue;
    }

    public boolean isIncludeValue() {
        return includeValue;
    }

    public ItemListenerConfig asItemListenerConfig(SerializationService serializationService) {
        validate();

        ItemListenerConfig config;
        if (className != null) {
            config = new ItemListenerConfig(className, includeValue);
        } else {
            ItemListener implementation = serializationService.toObject(this.listenerImplementation);
            config = new ItemListenerConfig(implementation, includeValue);
        }
        return config;
    }

    public static ItemListenerConfigHolder of(ItemListenerConfig config,
                                              SerializationService serializationService) {
        if (config.getImplementation() != null) {
            Data implementation = serializationService.toData(config.getImplementation());
            return new ItemListenerConfigHolder(implementation, config.isIncludeValue());
        } else {
            return new ItemListenerConfigHolder(config.getClassName(), config.isIncludeValue());
        }
    }
}
