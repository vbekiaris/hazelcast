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

import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.OperationAccessor;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.Version;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.spi.Operation.BITMASK_CUSTOM_OPERATION_FLAG;
import static com.hazelcast.test.HazelcastTestSupport.randomName;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class AuthorizationOpTest {

    private SerializationService serializationService;
    private String groupName;
    private String groupPassword;
    private Version[] advertisedVersions;

    @Before
    public void setup() {
        groupName = randomName();
        groupPassword = randomName();
        advertisedVersions = new Version[] {Version.of(10, 10), Version.UNKNOWN};
        serializationService = new DefaultSerializationServiceBuilder().build();
    }

    @Test
    public void whenFlagSet_additionalFieldsAreDeserialized() {
        AuthorizationOp op = new AuthorizationOp(groupName, groupPassword, advertisedVersions);
        Data serializedOp = serializationService.toData(op);
        AuthorizationOp deserializedOp = serializationService.toObject(serializedOp);

        assertTrue("Custom operation flag should have been set", OperationAccessor.isFlagSet(deserializedOp,
                BITMASK_CUSTOM_OPERATION_FLAG));
        assertEquals(groupName, deserializedOp.getGroupName());
        assertEquals(groupPassword, deserializedOp.getGroupPassword());
        assertArrayEquals(advertisedVersions, deserializedOp.getAdvertisedProtocols());
    }

    @Test
    public void whenFlagNotSet_additionalFieldsNotDeserialized() {
        AuthorizationOp op = new AuthorizationOp(groupName, groupPassword, advertisedVersions);
        // unset the custom flag
        OperationAccessor.setFlag(op, false, BITMASK_CUSTOM_OPERATION_FLAG);
        Data serializedOp = serializationService.toData(op);
        AuthorizationOp deserializedOp = serializationService.toObject(serializedOp);

        assertFalse("Custom operation flag should not have been set", OperationAccessor.isFlagSet(deserializedOp,
                BITMASK_CUSTOM_OPERATION_FLAG));
        assertEquals(groupName, deserializedOp.getGroupName());
        assertEquals(groupPassword, deserializedOp.getGroupPassword());
        assertNull(deserializedOp.getAdvertisedProtocols());
    }

}
