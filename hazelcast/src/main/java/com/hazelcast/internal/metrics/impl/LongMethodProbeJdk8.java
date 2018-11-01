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

package com.hazelcast.internal.metrics.impl;

import com.hazelcast.internal.metrics.LongProbeFunction;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.util.counters.Counter;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Semaphore;

import static com.hazelcast.internal.metrics.impl.ProbeUtils.TYPE_COLLECTION;
import static com.hazelcast.internal.metrics.impl.ProbeUtils.TYPE_COUNTER;
import static com.hazelcast.internal.metrics.impl.ProbeUtils.TYPE_LONG_NUMBER;
import static com.hazelcast.internal.metrics.impl.ProbeUtils.TYPE_MAP;
import static com.hazelcast.internal.metrics.impl.ProbeUtils.TYPE_PRIMITIVE_LONG;
import static com.hazelcast.internal.metrics.impl.ProbeUtils.TYPE_SEMAPHORE;
import static com.hazelcast.util.ExceptionUtil.rethrow;

public class LongMethodProbeJdk8<S> extends MethodProbe implements LongProbeFunction<S> {

    private static final MethodHandles.Lookup LOOKUP = MethodHandles.lookup();

    private final MethodHandle methodHandle;

    public LongMethodProbeJdk8(Method method, Probe probe, int type) {
        super(method, probe, type);
        try {
            methodHandle = LOOKUP.unreflect(method);
        } catch (IllegalAccessException e) {
            throw rethrow(e);
        }
    }

    @Override
    public long get(S source) throws Throwable {
        switch (type) {
            case TYPE_PRIMITIVE_LONG:
                return ((Number) methodHandle.invoke(source)).longValue();
            case TYPE_LONG_NUMBER:
                Number longNumber = (Number) methodHandle.invoke(source);
                return longNumber == null ? 0 : longNumber.longValue();
            case TYPE_MAP:
                Map<?, ?> map = (Map<?, ?>) methodHandle.invoke(source);
                return map == null ? 0 : map.size();
            case TYPE_COLLECTION:
                Collection<?> collection = (Collection<?>) methodHandle.invoke(source);
                return collection == null ? 0 : collection.size();
            case TYPE_COUNTER:
                Counter counter = (Counter) methodHandle.invoke(source);
                return counter == null ? 0 : counter.get();
            case TYPE_SEMAPHORE:
                Semaphore semaphore = (Semaphore) methodHandle.invoke(source);
                return semaphore == null ? 0 : semaphore.availablePermits();
            default:
                throw new IllegalStateException("Unrecognized type:" + type);
        }
    }
}
