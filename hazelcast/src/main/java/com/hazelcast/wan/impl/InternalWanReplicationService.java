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

import com.hazelcast.version.MemberVersion;
import com.hazelcast.version.Version;
import com.hazelcast.wan.WanReplicationService;

import javax.annotation.Nonnull;

public interface InternalWanReplicationService extends WanReplicationService {

    /**
     * Executed on the target WAN endpoint to select the most preferred protocol advertised by the
     * WAN source that is also supported by this Hazelcast instance.
     * If none match, then {@link com.hazelcast.internal.cluster.impl.VersionMismatchException} is thrown.
     * Unknown protocols are ignored.
     *
     * @param advertisedProtocols   protocols advertised by the connection initiator in order of preference
     * @return                      the most preferred advertised protocol that is supported by
     *                              this Hazelcast instance. If none matches,
     *                              {@link com.hazelcast.internal.cluster.impl.VersionMismatchException} is thrown
     */
    Version selectProtocol(@Nonnull Version[] advertisedProtocols, Version sourceClusterVersion,
                           MemberVersion sourceMemberVersion);
}
