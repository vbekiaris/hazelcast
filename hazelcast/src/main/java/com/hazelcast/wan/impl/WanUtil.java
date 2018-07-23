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

import com.hazelcast.internal.cluster.impl.VersionMismatchException;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.version.Version;
import com.hazelcast.wan.WanReplicationPublisher;
import com.hazelcast.wan.WanReplicationService;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.ServiceLoader;
import java.util.Set;

import static java.util.Collections.unmodifiableSet;

public final class WanUtil {

    private WanUtil() {
    }

    public static Set<Version> allSupportedProtocols(Collection<? extends WanReplicationPublisher> publishers) {
        Set<Version> supportedProtocols = new HashSet<Version>();
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

    public static VersionMismatchException newVersionMismatchException(Set<Version> supportedProtocols,
                                                                       Version[] advertisedProtocols) {
        return new VersionMismatchException(String.format("Could not locate a compatible WAN protocol. This member supports %s"
                        + " while remote member supports %s", supportedProtocols.toString(),
                Arrays.toString(advertisedProtocols)));
    }

    // scan WanReplicationPublishers registered via java.util.ServiceLoader for supported WAN protocols
    public static Set<Version> scanForSupportedVersions(ClassLoader configClassLoader) {
        Set<Version> supportedProtocols = new HashSet<Version>();
        ServiceLoader<WanReplicationPublisher> publishersLoader = ServiceLoader.load(WanReplicationPublisher.class,
                configClassLoader);
        Iterator<WanReplicationPublisher> registeredPublishers = publishersLoader.iterator();

        while (registeredPublishers.hasNext()) {
            WanReplicationPublisher publisher = registeredPublishers.next();
            supportedProtocols.addAll(publisher.getSupportedProtocols());
        }
        return supportedProtocols;
    }
}
