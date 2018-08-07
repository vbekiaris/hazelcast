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

package com.hazelcast.internal.cluster.impl.operations;

import com.hazelcast.config.GroupConfig;
import com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.OperationAccessor;
import com.hazelcast.util.Preconditions;
import com.hazelcast.version.MemberVersion;
import com.hazelcast.version.Version;
import com.hazelcast.wan.WanReplicationService;
import com.hazelcast.wan.impl.InternalWanReplicationService;

import java.io.IOException;

import static com.hazelcast.util.collection.ArrayUtils.createCopy;

public class AuthorizationOp extends AbstractJoinOperation {

    private static final int MAX_ADVERTISED_PROTOCOLS_COUNT = 1 << 8;

    private String groupName;
    private String groupPassword;
    private Version sourceClusterVersion;
    private MemberVersion sourceMemberVersion;
    /**
     * Advertised WAN protocol versions: sent from WAN connection initiator as plain strings
     * (instead of eg enum values) to avoid serialization failures. A 3.11 member sets the
     * {@link #BITMASK_CUSTOM_OPERATION_FLAG} to indicate advertised protocols are included
     * in the serialized operation. Previous version members will ignore the extra bytes.
     *
     * @since 3.11
     */
    private Version[] advertisedProtocols;
    private Object response = Boolean.TRUE;

    public AuthorizationOp() {
    }

    public AuthorizationOp(String groupName, String groupPassword, Version sourceClusterVersion,
                           MemberVersion sourceMemberVersion,
                           Version[] advertisedProtocols) {
        Preconditions.checkTrue(advertisedProtocols.length < MAX_ADVERTISED_PROTOCOLS_COUNT,
                "Maximum number of advertised protocols must be less than 256");
        this.groupName = groupName;
        this.groupPassword = groupPassword;
        this.sourceClusterVersion = sourceClusterVersion;
        this.sourceMemberVersion = sourceMemberVersion;
        this.advertisedProtocols = createCopy(advertisedProtocols);
        // Use the custom operation flag to indicate that AuthorizationOp serialized by this member
        // will include advertised protocols information
        OperationAccessor.setFlag(this, true, BITMASK_CUSTOM_OPERATION_FLAG);
    }

    @Override
    public void run() {
        boolean authorized = true;
        GroupConfig groupConfig = getNodeEngine().getConfig().getGroupConfig();
        if (!groupName.equals(groupConfig.getName()) ||
                !groupPassword.equals(groupConfig.getPassword())) {
            authorized = false;
        }
        // operation received from <= 3.10 member, expects a boolean response
        if (!OperationAccessor.isFlagSet(this, BITMASK_CUSTOM_OPERATION_FLAG)) {
            response = authorized;
            return;
        }
        // operation received from 3.11+ member
        if (!authorized) {
            response = new AuthorizationResponse(false);
            return;
        }
        InternalWanReplicationService wanReplicationService = getNodeEngine().getService(WanReplicationService.SERVICE_NAME);
        Version selectedProtocol = wanReplicationService.selectProtocol(advertisedProtocols,
                sourceClusterVersion, sourceMemberVersion);
        response = new AuthorizationResponse(true, selectedProtocol, getNodeEngine().getClusterService().getClusterVersion(),
                getNodeEngine().getVersion());
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        groupName = in.readUTF();
        groupPassword = in.readUTF();
        // check if supported protocols are available
        if (OperationAccessor.isFlagSet(this, BITMASK_CUSTOM_OPERATION_FLAG)) {
            sourceMemberVersion = in.readObject();
            sourceClusterVersion = in.readObject();
            int protocolCount = in.readUnsignedByte();
            advertisedProtocols = new Version[protocolCount];
            for (int i = 0; i < protocolCount; i++) {
                advertisedProtocols[i] = in.readObject();
            }
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeUTF(groupName);
        out.writeUTF(groupPassword);
        out.writeObject(sourceMemberVersion);
        out.writeObject(sourceClusterVersion);
        out.writeByte(advertisedProtocols.length);
        for (int i = 0; i < advertisedProtocols.length; i++) {
            out.writeObject(advertisedProtocols[i]);
        }
    }

    @Override
    public int getId() {
        return ClusterDataSerializerHook.AUTHORIZATION;
    }

    // for testing
    String getGroupName() {
        return groupName;
    }

    String getGroupPassword() {
        return groupPassword;
    }

    Version[] getAdvertisedProtocols() {
        return advertisedProtocols;
    }

    public Version getSourceClusterVersion() {
        return sourceClusterVersion;
    }

    public MemberVersion getSourceMemberVersion() {
        return sourceMemberVersion;
    }
}
