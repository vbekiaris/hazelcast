/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.wan.impl;

import com.hazelcast.nio.Address;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.wan.WanReplicationPublisher;
import com.hazelcast.wan.WanReplicationService;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static java.util.Collections.unmodifiableSet;

public final class WanUtil {

    private WanUtil() {
    }

    public static Set<String> allSupportedProtocols(Collection<? extends WanReplicationPublisher> publishers) {
        Set<String> supportedProtocols = new HashSet<String>();
        for (WanReplicationPublisher publisher : publishers) {
            supportedProtocols.addAll(publisher.getSupportedProtocols());
        }
        return unmodifiableSet(supportedProtocols);
    }

    public static InternalCompletableFuture invokeOnWanTarget(OperationService operationService, Operation operation,
                                                              Address target, long reponseTimeoutMillis) {
        String serviceName = WanReplicationService.SERVICE_NAME;
        return operationService.createInvocationBuilder(serviceName, operation, target)
                                .setTryCount(1)
                                .setCallTimeout(reponseTimeoutMillis)
                                .invoke();
    }
}
