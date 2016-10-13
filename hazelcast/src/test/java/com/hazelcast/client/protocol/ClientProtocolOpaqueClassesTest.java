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

package com.hazelcast.client.protocol;

import com.hazelcast.client.impl.protocol.ClientProtocol;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.Set;

import static com.hazelcast.test.ReflectionsHelper.REFLECTIONS;
import static org.junit.Assert.fail;

/**
 * Tests for classes which are used as Data in the client protocol
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientProtocolOpaqueClassesTest {

    // verify that any class which is DataSerializable and is not annotated with @ClientProtocol
    // is also an IdentifiedDataSerializable
    @Test
    public void test_dataSerializableClasses_areIdentifiedDataSerializable() {
        Set allDataSerializableClasses = REFLECTIONS.getSubTypesOf(DataSerializable.class);
        Set allIdDataSerializableClasses = REFLECTIONS.getSubTypesOf(IdentifiedDataSerializable.class);

        allDataSerializableClasses.removeAll(allIdDataSerializableClasses);

        // locate all classes annotated with ClientProtocol (and their subclasses) and remove those as well
        Set allAnnotatedClasses = REFLECTIONS.getTypesAnnotatedWith(ClientProtocol.class);
        allDataSerializableClasses.removeAll(allAnnotatedClasses);

        // since test classes are included in the classpath, remove them
        Set testClasses = new HashSet();
        for (Object o : allDataSerializableClasses) {
            if (o.toString().contains("Test")) {
                testClasses.add(o);
            }
        }

        allDataSerializableClasses.removeAll(testClasses);

        if (allDataSerializableClasses.size() > 0) {
            System.out.println("The following classes are DataSerializable while they shoudl be  IdentifiedDataSerializable:");
            // failure - output non-compliant classes to standard output and fail the test
            for (Object o : allDataSerializableClasses) {
                System.out.println(o.toString());
            }
            fail("There are " + allDataSerializableClasses.size() + " classes which are DataSerializable, not @ClientProtocol-"
                    + "annotated and are not IdentifiedDataSerializable.");
        }
    }

}
