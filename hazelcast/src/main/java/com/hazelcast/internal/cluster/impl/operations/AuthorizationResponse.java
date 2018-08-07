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

import com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.version.MemberVersion;
import com.hazelcast.version.Version;

import java.io.IOException;

/**
 * Response object returned from {@link AuthorizationOp}, when target endpoint is Hazelcast 3.11+
 *
 * @since 3.11
 */
public class AuthorizationResponse implements IdentifiedDataSerializable {

    private boolean authorized;
    private Version selectedWanProtocolVersion;
    private Version clusterVersion;
    private MemberVersion memberVersion;

    public AuthorizationResponse() {
    }

    public AuthorizationResponse(boolean authorized) {
        this(authorized, null, null, null);
    }

    public AuthorizationResponse(boolean authorized, Version selectedWanProtocolVersion, Version clusterVersion,
                                 MemberVersion memberVersion) {
        this.authorized = authorized;
        this.selectedWanProtocolVersion = selectedWanProtocolVersion;
        this.clusterVersion = clusterVersion;
        this.memberVersion = memberVersion;
    }

    @Override
    public int getFactoryId() {
        return ClusterDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return ClusterDataSerializerHook.AUTHORIZATION_RESPONSE;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        out.writeBoolean(authorized);
        if (authorized) {
            out.writeObject(selectedWanProtocolVersion);
            out.writeObject(clusterVersion);
            out.writeObject(memberVersion);
        }
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        authorized = in.readBoolean();
        if (authorized) {
            selectedWanProtocolVersion = in.readObject();
            clusterVersion = in.readObject();
            memberVersion = in.readObject();
        }
    }

    public boolean isAuthorized() {
        return authorized;
    }

    public Version getSelectedWanProtocolVersion() {
        return selectedWanProtocolVersion;
    }

    public Version getClusterVersion() {
        return clusterVersion;
    }

    public MemberVersion getMemberVersion() {
        return memberVersion;
    }
}
