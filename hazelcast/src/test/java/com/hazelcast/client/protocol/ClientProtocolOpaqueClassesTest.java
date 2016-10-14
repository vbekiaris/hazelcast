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
import java.util.SortedSet;
import java.util.TreeSet;

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
        Set dataSerializableClasses = REFLECTIONS.getSubTypesOf(DataSerializable.class);
        Set allIdDataSerializableClasses = REFLECTIONS.getSubTypesOf(IdentifiedDataSerializable.class);

        dataSerializableClasses.removeAll(allIdDataSerializableClasses);
        // also remove IdentifiedDataSerializable itself
        dataSerializableClasses.remove(IdentifiedDataSerializable.class);

        // locate all classes annotated with ClientProtocol (and their subclasses) and remove those as well
        Set allAnnotatedClasses = REFLECTIONS.getTypesAnnotatedWith(ClientProtocol.class);
        dataSerializableClasses.removeAll(allAnnotatedClasses);

        // since test classes are included in the classpath, remove them
        Set testClasses = new HashSet();
        for (Object o : dataSerializableClasses) {
            if (o.toString().contains("Test") || o.toString().contains("Mock")) {
                testClasses.add(o);
            }
        }

        dataSerializableClasses.removeAll(testClasses);

        if (dataSerializableClasses.size() > 0) {
            SortedSet<String> nonCompliantClassNames = new TreeSet<String>();
            for (Object o : dataSerializableClasses) {
                nonCompliantClassNames.add(o.toString());
            }
            System.out.println("The following classes are DataSerializable while they should be IdentifiedDataSerializable:");
            // failure - output non-compliant classes to standard output and fail the test
            for (String s : nonCompliantClassNames) {
                System.out.println(s);
            }
            fail("There are " + dataSerializableClasses.size() + " classes which are DataSerializable, not @ClientProtocol-"
                    + "annotated and are not IdentifiedDataSerializable.");
        }
    }

}
