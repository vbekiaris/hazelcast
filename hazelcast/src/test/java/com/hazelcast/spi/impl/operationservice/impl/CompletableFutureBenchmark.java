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

package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.internal.longregister.LongRegisterService;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastTestSupport;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@SuppressWarnings("unused")
public class CompletableFutureBenchmark
        extends HazelcastTestSupport {

    private static final int WARMUP_ITERATIONS_COUNT = 10;
    private static final int MEASUREMENT_ITERATIONS_COUNT = 100;

    private HazelcastInstance hz;
    private HazelcastInstance hz2;
    private IAtomicLong atomicLong;
    private IMap<Integer, String> map;

    @Setup
    public void setup() {
        // config
        Config config = new Config();
        // disable network
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true).addMember("127.0.0.1");

        hz = Hazelcast.newHazelcastInstance(config);
        System.out.println("---");
        hz2 = Hazelcast.newHazelcastInstance(config);
        while (true) {
            if (hz.getCluster().getMembers().size() == 2) {
                break;
            }
        }

        atomicLong = hz.getDistributedObject(LongRegisterService.SERVICE_NAME, "test");
        map = hz.getMap("test");
    }

    @TearDown
    public void tearDown() {
        hz.getLifecycleService().terminate();
        hz2.getLifecycleService().terminate();
    }

//    @Benchmark
//    public long atomic_long_ops() {
//        long d = 0;
//        for (int i = 0; i < 10_000; i++) {
//            d = atomicLong.getAndIncrement();
//        }
//        return d;
//    }

    @Benchmark
    public long iMapBenchmark() {
        long d = 0;
        for (int i = 0; i < 1000; i++) {
            final int c = i;
            CompletionStage<Void> setAsync = map.setAsync(i, ""+i);
            setAsync.thenRunAsync(() -> map.getAsync(c).toCompletableFuture().join());
        }
        return d;
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(CompletableFutureBenchmark.class.getSimpleName())
                .warmupIterations(WARMUP_ITERATIONS_COUNT)
                .warmupTime(TimeValue.milliseconds(2))
                .measurementIterations(MEASUREMENT_ITERATIONS_COUNT)
                .measurementTime(TimeValue.milliseconds(100))
                .addProfiler(GCProfiler.class)
                .output("/Users/vb/tmp/complfut")
                .forks(1)
                .build();

        new Runner(opt).run();
    }

}
