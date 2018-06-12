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

import java.io.IOException;
import java.util.Arrays;

public class AuthorizationOp extends AbstractJoinOperation {

    private String groupName;
    private String groupPassword;
    private String[] supportedProtocols;
    private Object response = Boolean.TRUE;

    public AuthorizationOp() {
    }

    public AuthorizationOp(String groupName, String groupPassword, String[] supportedProtocols) {
        this.groupName = groupName;
        this.groupPassword = groupPassword;
        this.supportedProtocols = Arrays.copyOf(supportedProtocols, supportedProtocols.length);
        OperationAccessor.setFlag(this, true, BITMASK_WAN_PROTOCOL_AUTH_OP);
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
        if (supportedProtocols != null) {
            response = supportedProtocols[0];
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
        if (OperationAccessor.isFlagSet(this, BITMASK_WAN_PROTOCOL_AUTH_OP)) {
            supportedProtocols = in.readUTFArray();
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeUTF(groupName);
        out.writeUTF(groupPassword);
        out.writeUTFArray(supportedProtocols);
    }

    @Override
    public int getId() {
        return ClusterDataSerializerHook.AUTHORIZATION;
    }
}
