package com.hazelcast.internal.metrics.impl;

import com.hazelcast.internal.RequiresJdk8;
import com.hazelcast.internal.metrics.LongProbeFunction;
import com.hazelcast.internal.metrics.Probe;

import java.lang.reflect.Method;
import java.util.function.Supplier;

public class LongMethodProbeJdk8<S> extends MethodProbe implements LongProbeFunction<S> {

    private final Supplier<Long> longValueSupplier = new Supplier<Long>() {
        @Override
        public Long get() {
            return 0L;
        }
    };

    public LongMethodProbeJdk8(Method method, Probe probe, int type) {
        super(method, probe, type);
    }

    @RequiresJdk8
    @Override
    public long get(S source) {
        return longValueSupplier.get();
    }
}
