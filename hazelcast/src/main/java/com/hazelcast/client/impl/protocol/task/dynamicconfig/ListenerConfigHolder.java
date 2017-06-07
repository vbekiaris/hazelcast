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

import com.hazelcast.config.ListenerConfig;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.serialization.SerializationService;

import java.util.EventListener;

public class ListenerConfigHolder {

    protected final String className;
    protected final Data listenerImplementation;

    public ListenerConfigHolder(String className) {
        this.className = className;
        this.listenerImplementation = null;
    }

    public ListenerConfigHolder(Data listenerImplementation) {
        this.className = null;
        this.listenerImplementation = listenerImplementation;
    }

    public String getClassName() {
        return className;
    }

    public Data getListenerImplementation() {
        return listenerImplementation;
    }

    public ListenerConfig asListenerConfig(SerializationService serializationService) {
        validate();
        if (className != null) {
            return new ListenerConfig(className);
        } else {
            EventListener eventListener = serializationService.toObject(listenerImplementation);
            return new ListenerConfig(eventListener);
        }
    }

    void validate() {
        if (className == null && listenerImplementation == null) {
            throw new IllegalArgumentException("One of class name or listener implementation must be not null");
        }
    }

    public static ListenerConfigHolder of(ListenerConfig config, SerializationService serializationService) {
        if (config.getClassName() != null) {
            return new ListenerConfigHolder(config.getClassName());
        } else {
            Data implementationData = serializationService.toData(config.getImplementation());
            return  new ListenerConfigHolder(implementationData);
        }
    }
}
