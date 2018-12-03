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

package com.hazelcast.internal.cluster.impl;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

public enum ProtocolType {
    MEMBER(1),
    CLIENT(1),
    WAN(Integer.MAX_VALUE),
    TEXT(Integer.MAX_VALUE);

    private static final Set<ProtocolType> ALL_PROTOCOL_TYPES;

    static {
        Set<ProtocolType> allProtocolTypes = EnumSet.allOf(ProtocolType.class);
        ALL_PROTOCOL_TYPES = Collections.unmodifiableSet(allProtocolTypes);
    }

    public static ProtocolType valueOf(int ordinal) {
        return ProtocolType.values()[ordinal];
    }

    public static Set<ProtocolType> valuesAsSet() {
        return ALL_PROTOCOL_TYPES;
    }

    private final int listeningSocketCardinality;

    ProtocolType(int listeningSocketCardinality) {
        this.listeningSocketCardinality = listeningSocketCardinality;
    }

    public int getListeningSocketCardinality() {
        return listeningSocketCardinality;
    }
}
