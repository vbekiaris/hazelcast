/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.core.server;

import com.hazelcast.config.Config;
import com.hazelcast.config.DiscoveryStrategyConfig;
import com.hazelcast.config.ServerSocketEndpointConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.properties.ClusterProperty;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Map;

import static com.hazelcast.internal.nio.IOUtil.closeResource;
import static com.hazelcast.spi.discovery.uds.UDSDiscoveryStrategyFactory.UDS_SOCKET_DIRECTORY;

/**
 * Starts a Hazelcast Member.
 */
public final class HazelcastMemberStarter {

    private HazelcastMemberStarter() {
    }

    /**
     * Creates a server instance of Hazelcast.
     * <p>
     * If user sets the system property "print.port", the server writes the port number of the Hazelcast instance to a file.
     * The file name is the same as the "print.port" property.
     *
     * @param args none
     */
    public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException {
        System.setProperty("hazelcast.tracking.server", "true");

        Config config = new Config();
        config.getMetricsConfig().setEnabled(false);
        config.getAdvancedNetworkConfig()
              .setEnabled(true);
        // issue with advanced networking + no ClIENT endpoint
        config.getAdvancedNetworkConfig().setClientEndpointConfig(
                new ServerSocketEndpointConfig().setPort(5555)
        );
        config.getAdvancedNetworkConfig().getJoin().getDiscoveryConfig()
              .addDiscoveryStrategyConfig(
                      new DiscoveryStrategyConfig("com.hazelcast.spi.discovery.uds.UDSDiscoveryStrategy",
                              Map.of(UDS_SOCKET_DIRECTORY, System.getProperty("hazelcast.disco.socket.directory",
                                      "/Users/vb/tmp/socket"))));
        config.setProperty(ClusterProperty.DISCOVERY_SPI_ENABLED.getName(), "true");
        HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);
        printMemberPort(hz);
    }

    private static void printMemberPort(HazelcastInstance hz) throws FileNotFoundException, UnsupportedEncodingException {
        String printPort = System.getProperty("print.port");
        if (printPort != null) {
            PrintWriter printWriter = null;
            try {
                printWriter = new PrintWriter("ports" + File.separator + printPort, "UTF-8");
                printWriter.println(hz.getCluster().getLocalMember().getAddress().getPort());
            } finally {
                closeResource(printWriter);
            }
        }
    }
}
