/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.cluster.impl;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

/**
 * Wrapper object indicating a change in the cluster state - it may be state-flag chance, version-change or both
 */
public class ClusterStateChange<T> implements DataSerializable {

    private Class<T> type;
    private T newState;

    public ClusterStateChange() {
    }

    @SuppressWarnings("unchecked")
    public ClusterStateChange(T newState) {
        this.type = (Class<T>) newState.getClass();
        this.newState = newState;
    }

    public Class<T> getType() {
        return type;
    }

    public T getNewState() {
        return newState;
    }

    public <T> boolean isOfType(Class<T> type) {
        return this.type.equals(type);
    }

    public void validate() {
        if (type == null || newState == null) {
            throw new IllegalArgumentException("Invalid null state");
        }

        if (isOfType(ClusterState.class)) {
            if (newState == ClusterState.IN_TRANSITION) {
                throw new IllegalArgumentException("IN_TRANSITION is an internal state!");
            }
        }
    }

    public static <T> ClusterStateChange<T> from(T object) {
        return new ClusterStateChange<T>(object);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(type);
        out.writeObject(newState);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        type = in.readObject();
        newState = in.readObject();
    }

}
