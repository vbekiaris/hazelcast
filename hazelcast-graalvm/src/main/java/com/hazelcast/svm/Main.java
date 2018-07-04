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

package com.hazelcast.svm;

import com.hazelcast.config.Config;
import com.hazelcast.config.FileSystemXmlConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.spi.properties.GroupProperty;

import java.io.File;
import java.util.concurrent.locks.LockSupport;

import static com.hazelcast.core.LifecycleEvent.LifecycleState.SHUTDOWN;
import static java.lang.System.nanoTime;

public class Main {
    private static volatile boolean RUNNING = true;

    public static void main(String[] args) {
        long start = nanoTime();
        System.err.println("Starting Hazelcast");
        Config config = getConfigFromFile();
        if (config == null) {
            config = new Config();
            config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
            config.getNetworkConfig().getJoin()
                  .getTcpIpConfig()
                  .setEnabled(true)
                  .addMember("10.216.1.39");
            config.setProperty(GroupProperty.PHONE_HOME_ENABLED.getName(), "false");
            config.setProperty(GroupProperty.LOGGING_TYPE.getName(), "jdk");
        }
        HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);
        System.err.println("Hazelcast startup completed in " + ((nanoTime() - start) / 1000000) + "ms");

        hz.getLifecycleService().addLifecycleListener(new LifecycleListener() {
            public void stateChanged(LifecycleEvent event) {
                if (event.getState() == SHUTDOWN) {
                    System.err.println("Shutdown complete.");
                    RUNNING = false;
                }
            }
        });

        while (RUNNING) {
            System.out.flush();
            System.err.flush();
            LockSupport.parkNanos(500000000);
        }
        System.exit(0);

    }

    private static Config getConfigFromFile() {
        File configFile = new File("./hazelcast.xml");
        try {
            FileSystemXmlConfig config = new FileSystemXmlConfig(configFile);
            return config;
        } catch (Exception e) {
            System.err.println("Could not read configuration file " + configFile.getAbsolutePath());
            return null;
        }

    }
}
