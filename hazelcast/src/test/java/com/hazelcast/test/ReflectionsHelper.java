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
package com.hazelcast.test;

import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;

/**
 * Initialize once org.reflections library to avoid duplicate work scanning the classpath from individual tests.
 * Reflections is configured with the {@code TEST_CLASSES_CLASSLOADER}. A quick way to test whether a {@code Class} is loaded
 * from production or test classes is to check whether {code klass.getClassLoader().equals(TEST_CLASSES_CLASSLOADER)} is true.
 */
public class ReflectionsHelper {

    public static final Reflections REFLECTIONS;
    // classes in the test classpath are loaded by this classloader, which delegates to the parent classloader _after_ attempting
    // to load the classes itself.
    public static final ClassLoader TEST_CLASSES_CLASSLOADER;

    static {
        File testClassesDirectory = new File("target/test-classes");

        URL productionClassesURL = null;
        try {
            productionClassesURL = testClassesDirectory.toURI().toURL();
        } catch (MalformedURLException e) {
            throw new AssertionError(
                    "Could not obtain a URL for the compiled test classes classpath at " + testClassesDirectory
                            .getAbsolutePath());
        }

        TEST_CLASSES_CLASSLOADER = new PostDelegationClassLoader(new URL[]{productionClassesURL},
                ClasspathHelper.staticClassLoader());
        REFLECTIONS = new Reflections(new ConfigurationBuilder()
                .addUrls(ClasspathHelper.forPackage("com.hazelcast", TEST_CLASSES_CLASSLOADER))
                .addClassLoader(TEST_CLASSES_CLASSLOADER)
                .addScanners(new SubTypesScanner()).addScanners(new TypeAnnotationsScanner()));
    }

    // a URLClassLoader that delegates to the parent classloader _after_ having attempted to
    // locate the class in its configured URLs
    public static class PostDelegationClassLoader extends URLClassLoader {

        public PostDelegationClassLoader(URL[] urls, ClassLoader parent) {
            super(urls, parent);
        }

        public Class<?> loadClass(String name) throws ClassNotFoundException {
            Class loadedClass = findLoadedClass(name);
            if (loadedClass == null) {
                try {
                    // first try loading the class from the configured URLs
                    loadedClass = findClass(name);
                } catch (ClassNotFoundException e) {
                }

                // If not found locally, delegate to parent via URLClassLoader.loadClass
                if (loadedClass == null) {
                    // do not catch ClassNotFoundException here
                    loadedClass = super.loadClass(name);
                }
            }
            return loadedClass;
        }
    }

    public static boolean isTestClass(Class klass) {
        return klass.getClassLoader().equals(TEST_CLASSES_CLASSLOADER);
    }
}
