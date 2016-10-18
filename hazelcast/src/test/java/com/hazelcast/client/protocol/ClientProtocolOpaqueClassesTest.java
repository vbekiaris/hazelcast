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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;
import com.hazelcast.client.impl.protocol.ClientProtocol;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import static com.hazelcast.test.ReflectionsHelper.REFLECTIONS;
import static com.hazelcast.test.ReflectionsHelper.TEST_CLASSES_CLASSLOADER;
import static com.hazelcast.test.ReflectionsHelper.isTestClass;
import static org.junit.Assert.fail;

/**
 * Tests for classes which are used as Data in the client protocol
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
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
        Set allAnnotatedClasses = REFLECTIONS.getTypesAnnotatedWith(ClientProtocol.class, false);
        dataSerializableClasses.removeAll(allAnnotatedClasses);

        // since test classes are included in the classpath, remove them
        Set testClasses = new HashSet();
        for (Object o : dataSerializableClasses) {
            if (isTestClass((Class) o)) {
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

    @Test
    public void test_subclassesOfAnnotatedClasses_areAnnotated() {
        // locate all classes annotated with ClientProtocol (and their subclasses) and remove those as well
        Set<Class<?>> directlyAnnotatedClasses = REFLECTIONS.getTypesAnnotatedWith(ClientProtocol.class, true);
        Set<String> notAnnotatedSubclasses = new TreeSet<String>();

        for (Class klass : directlyAnnotatedClasses) {
            Set subclasses = REFLECTIONS.getSubTypesOf(klass);
            for (Object subclass : subclasses) {
                if (!((Class) subclass).isAnnotationPresent(ClientProtocol.class) && !isTestClass((Class) subclass)) {
                    notAnnotatedSubclasses.add(subclass.toString());
                }
            }
        }

        if (!notAnnotatedSubclasses.isEmpty()) {
            System.out.println("The following classes are subclasses of an annotated interface or class but do not bear the "
                    + "annotation themselves");
            for (String s : notAnnotatedSubclasses) {
                System.out.println(s);
            }
            fail("There are " + notAnnotatedSubclasses.size() + " subclasses of @ClientProtocol annotated classes");
        }
    }

    @Test
    public void test_identifiedDataSerializables_haveUniqueFactoryAndTypeId()
            throws IllegalAccessException, InvocationTargetException {
        Set<String> classesWithInstantiationProblems = new TreeSet<String>();

        Multimap<Integer, Integer> factoryToTypeId = HashMultimap.create();

        Set<Class<? extends IdentifiedDataSerializable>> identifiedDataSerializables = REFLECTIONS
                .getSubTypesOf(IdentifiedDataSerializable.class);

        for (Class<? extends IdentifiedDataSerializable> klass : identifiedDataSerializables) {
            // only test production classes
            if (!isTestClass(klass) && !klass.isInterface() && !Modifier.isAbstract(klass.getModifiers())) {
                // wrap all of this in try-catch, as it is legitimate for some classes to throw UnsupportedOperationException
                try {
                    Constructor<? extends IdentifiedDataSerializable> ctor = klass.getDeclaredConstructor();
                    ctor.setAccessible(true);
                    IdentifiedDataSerializable instance = ctor.newInstance();
                    int factoryId = instance.getFactoryId();
                    int typeId = instance.getId();
                    if (factoryToTypeId.containsEntry(factoryId, typeId)) {
                        fail("Factory-Type ID pair {" + factoryId + ", " + typeId + "} from " + klass.toString() + " is already "
                                + "registered in another type.");
                    } else {
                        factoryToTypeId.put(factoryId, typeId);
                    }
                } catch (UnsupportedOperationException e) {
                    // expected from some classes
                } catch (InstantiationException e) {
                    classesWithInstantiationProblems.add(klass.getName());
                    e.printStackTrace();
                } catch (NoSuchMethodException e) {
                    classesWithInstantiationProblems.add(klass.getName());
                    e.printStackTrace();
                }
            }
        }

        if (!classesWithInstantiationProblems.isEmpty()) {
            System.out.println("There are " + classesWithInstantiationProblems.size() + " classes which threw an exception while "
                    + "attempting to invoke a default no-args constructor. See console output for exception details. List of "
                    + "problematic classes:");
            for (String className : classesWithInstantiationProblems) {
                System.out.println(className);
            }
        }
    }

}
