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
import com.hazelcast.version.Version;
import com.hazelcast.wan.WanReplicationService;

import java.io.IOException;

import static com.hazelcast.util.collection.ArrayUtils.createCopy;

public class AuthorizationOp extends AbstractJoinOperation {

    private String groupName;
    private String groupPassword;
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

    public AuthorizationOp(String groupName, String groupPassword, Version[] advertisedProtocols) {
        this.groupName = groupName;
        this.groupPassword = groupPassword;
        this.advertisedProtocols = createCopy(advertisedProtocols);
        // Use the custom operation flag to indicate that AuthorizationOp serialized by this member
        // will include advertised protocols information
        OperationAccessor.setFlag(this, true, BITMASK_CUSTOM_OPERATION_FLAG);
    }

    @Override
    public void run() {
        GroupConfig groupConfig = getNodeEngine().getConfig().getGroupConfig();
        if (!groupName.equals(groupConfig.getName())) {
            response = Boolean.FALSE;
        } else if (!groupPassword.equals(groupConfig.getPassword())) {
            response = Boolean.FALSE;
        }
        // 3.11+, select most preferred protocol
        if (advertisedProtocols != null && advertisedProtocols.length > 0) {
            WanReplicationService wanReplicationService = getNodeEngine().getWanReplicationService();
            response = wanReplicationService.selectProtocol(advertisedProtocols);
        }
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
        out.writeByte(advertisedProtocols.length);
        for (int i = 0; i < advertisedProtocols.length; i++) {
            out.writeObject(advertisedProtocols[i]);
        }
    }

    @Override
    public int getId() {
        return ClusterDataSerializerHook.AUTHORIZATION;
    }
}
