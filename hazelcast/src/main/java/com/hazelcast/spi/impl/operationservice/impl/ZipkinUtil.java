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

package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.nio.Address;
import zipkin.Endpoint;

import java.net.UnknownHostException;

/**
 *
 */
public class ZipkinUtil {

    /**
     *
     * @param address
     * @return
     */
    public static Endpoint.Builder forAddress(Address address) {
        try {
            Endpoint.Builder endpointBuilder = Endpoint.builder();
            if (address.isIPv4()) {
                byte[] ipv4Address = address.getInetAddress().getAddress();
                int packedAddress =
                        (ipv4Address[0] << 24 & 0xff000000) | (ipv4Address[1] << 16 & 0xff0000) | (
                                ipv4Address[2] << 8 & 0xff00) | (ipv4Address[3] & 0xff);
                endpointBuilder.ipv4(packedAddress);
            } else {
                endpointBuilder.ipv6(address.getInetAddress().getAddress());
            }
            endpointBuilder.port(address.getPort());
            return endpointBuilder;
        } catch (UnknownHostException e) {
            throw new RuntimeException("Could not resolve address for " + address, e);
        }
    }
}
