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

package com.hazelcast.test;

public final class TestEnvironment {

    public static final String HAZELCAST_TEST_USE_NETWORK = "hazelcast.test.use.network";
    // if defined, should indicate an existing path where files will be created to dump the names of classes
    // which were actually serialized/deserialized during test suite execution. One file per JVM is created.
    public static final String RECORD_SERIALIZED_CLASS_NAMES = "hazelcast.test.record.serialized.class.names";

    private TestEnvironment() {
    }

    public static boolean isMockNetwork() {
        return !Boolean.getBoolean(HAZELCAST_TEST_USE_NETWORK);
    }

    public static boolean isRecordingSerializedClassNames() {
        return System.getProperty(RECORD_SERIALIZED_CLASS_NAMES) != null;
    }

    public static String getSerializedClassNamesPath() {
        return System.getProperty(RECORD_SERIALIZED_CLASS_NAMES);
    }
}
