/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.discovery.uds;

import com.hazelcast.cluster.Address;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.discovery.AbstractDiscoveryStrategy;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.SimpleDiscoveryNode;

import java.io.IOException;
import java.net.UnixDomainSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class UDSDiscoveryStrategy extends AbstractDiscoveryStrategy {

    private final Path socketDirectory;

    public UDSDiscoveryStrategy(ILogger logger, Map<String, Comparable> properties) {
        super(logger, properties);
        this.socketDirectory = Path.of((String) properties.get(UDSDiscoveryStrategyFactory.UDS_SOCKET_DIRECTORY));
    }

    @Override
    public Iterable<DiscoveryNode> discoverNodes() {
        try {
            var sockets = Files.list(socketDirectory);
            List<DiscoveryNode> nodes = sockets.map(path
                    -> new SimpleDiscoveryNode(toAddress(path))).collect(Collectors.toList());
            System.out.println(nodes);
            return nodes;
        } catch (IOException e) {
            throw new HazelcastException(e);
        }
    }

    private Address toAddress(Path path) {
        return new Address(UnixDomainSocketAddress.of(path));
    }
}
