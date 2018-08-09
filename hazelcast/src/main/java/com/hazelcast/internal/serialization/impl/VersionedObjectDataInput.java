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

package com.hazelcast.internal.serialization.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.version.Version;

import java.io.InputStream;

import static com.hazelcast.internal.cluster.Versions.LEGACY;
import static com.hazelcast.version.Version.UNKNOWN;

/**
 * Base class for ObjectDataInput that is VersionAware and allows mutating the version.
 * <p>
 * What the version means it's up to the Serializer/Deserializer.
 * If the serializer supports versioning it may set the version to use for the serialization on this object.
 */
abstract class VersionedObjectDataInput extends InputStream implements ObjectDataInput {

    protected Version version = UNKNOWN;

    /**
     * If the serializer supports versioning it may set the version to use for the serialization on this object.
     *
     * @param version version to set
     */
    public void setVersion(Version version) {
        this.version = version;
    }

    public Version getWanProtocolVersion() {
        if (LEGACY.equals(version)) {
            return LEGACY;
        }
        if (version.getMajor() < 0) {
            // WAN protocol version
            return Version.of(-1 * version.getMajor(), version.getMinor());
        }
        // no WAN protocol version was set
        return UNKNOWN;
    }

    /**
     * If the serializer supports versioning it may set the version to use for the serialization on this object.
     * <p>
     * This method makes the version available for the user.
     *
     * @return the version of {@code Version.UNKNOWN} if the version is unknown to the object
     */
    @Override
    public Version getVersion() {
        if (version.getMajor() < 0) {
            // a WAN protocol version is represented in this input's version field
            return UNKNOWN;
        }
        return version;
    }
}
