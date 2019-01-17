/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal;

import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.metrics.impl.LongMethodProbeJdk8;
import com.hazelcast.internal.metrics.impl.MethodProbe;
import com.hazelcast.internal.util.JavaVersion;

import java.lang.reflect.Method;

import static com.hazelcast.internal.util.JavaVersion.JAVA_1_8;

/**
 * Utility class to construct instances depending on the runtime platform.
 */
public class PlatformSpecific {

    private PlatformSpecific() {

    }

    public static <S> MethodProbe createLongMethodProbe(Method method, Probe probe, int type) {
        if (JavaVersion.isAtLeast(JAVA_1_8)) {
            return new LongMethodProbeJdk8<S>(method, probe, type);
        } else {
            return new MethodProbe.LongMethodProbe<S>(method, probe, type);
        }
    }

    // Ensures a JDK 6 build cannot be executed on JDK 8+
    // Required on startup in case MethodHandles usage is introduced, as the JDK 6 build cannot
    // run on JDK 8+
    public static void checkRuntime() {

    }
}
