/*
 * Copyright (c) 2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.test.starter;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;

public class HazelcastStarter {

    public static HazelcastInstance startHazelcastVersion(String version) {
        return startHazelcastVersion(version, null);
    }

    public static HazelcastInstance startHazelcastVersion(String version, Config configTemplate) {
        File versionDir = getOrCreateVersionVersionDirectory(version);
        File[] files = Downloader.downloadVersion(version, versionDir);
        URL[] urls = fileIntoUrls(files);
        ClassLoader parentClassloader = HazelcastStarter.class.getClassLoader();
        HazelcastAPIDelegatingClassloader classloader = new HazelcastAPIDelegatingClassloader(urls, parentClassloader);
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(null);
        try {
            Class<Hazelcast> hazelcastClass = (Class<Hazelcast>) classloader.loadClass("com.hazelcast.core.Hazelcast");
            System.out.println(hazelcastClass + " loaded by " + hazelcastClass.getClassLoader());
            Class<?> configClass = classloader.loadClass("com.hazelcast.config.Config");
            Object config;
            if (configTemplate == null) {
                config = configClass.newInstance();
                Method setClassLoaderMethod = configClass.getMethod("setClassLoader", ClassLoader.class);
                setClassLoaderMethod.invoke(config, classloader);
            } else {
                config = Configuration.configLoader(configTemplate, classloader);
            }

            Method newHazelcastInstanceMethod = hazelcastClass.getMethod("newHazelcastInstance", configClass);
            Object delegate = newHazelcastInstanceMethod.invoke(null, config);

            return HazelcastProxyFactory.proxy(delegate);

        } catch (ClassNotFoundException e) {
            throw Utils.rethrow(e);
        } catch (NoSuchMethodException e) {
            throw Utils.rethrow(e);
        } catch (IllegalAccessException e) {
            throw Utils.rethrow(e);
        } catch (InvocationTargetException e) {
            throw Utils.rethrow(e);
        } catch (InstantiationException e) {
            throw Utils.rethrow(e);
        } finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    public static HazelcastInstance startHazelcastClientVersion(String version) {
        File versionDir = getOrCreateVersionVersionDirectory(version);
        File[] files = Downloader.downloadVersion(version, versionDir);
        URL[] urls = fileIntoUrls(files);
        ClassLoader parentClassloader = HazelcastStarter.class.getClassLoader();
        HazelcastAPIDelegatingClassloader classloader = new HazelcastAPIDelegatingClassloader(urls, parentClassloader);
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(null);
        try {
            Class<Hazelcast> hazelcastClass = (Class<Hazelcast>) classloader.loadClass("com.hazelcast.client.HazelcastClient");
            System.out.println(hazelcastClass + " loaded by " + hazelcastClass.getClassLoader());
            Class<?> configClass = classloader.loadClass("com.hazelcast.client.config.ClientConfig");
            Object config = configClass.newInstance();
            Method setClassLoaderMethod = configClass.getMethod("setClassLoader", ClassLoader.class);
            setClassLoaderMethod.invoke(config, classloader);

            Method newHazelcastInstanceMethod = hazelcastClass.getMethod("newHazelcastClient", configClass);
            Object delegate = newHazelcastInstanceMethod.invoke(null, config);
            return HazelcastProxyFactory.proxy(delegate);

        } catch (ClassNotFoundException e) {
            throw Utils.rethrow(e);
        } catch (NoSuchMethodException e) {
            throw Utils.rethrow(e);
        } catch (IllegalAccessException e) {
            throw Utils.rethrow(e);
        } catch (InvocationTargetException e) {
            throw Utils.rethrow(e);
        } catch (InstantiationException e) {
            throw Utils.rethrow(e);
        } finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    private static URL[] fileIntoUrls(File[] files) {
        URL[] urls = new URL[files.length];
        for (int i = 0; i < files.length; i++) {
            try {
                urls[i] = files[i].toURL();
            } catch (MalformedURLException e) {
                throw Utils.rethrow(e);
            }
        }
        return urls;
    }

    private static File getOrCreateVersionVersionDirectory(String version) {
        File workingDir = Configuration.WORKING_DIRECTORY;
        if (!workingDir.isDirectory() || !workingDir.exists()) {
            throw new GuardianException("Working directory " + workingDir + " does not exist.");
        }

        File versionDir = new File(Configuration.WORKING_DIRECTORY, version);
        versionDir.mkdir();
        return versionDir;
    }
}
