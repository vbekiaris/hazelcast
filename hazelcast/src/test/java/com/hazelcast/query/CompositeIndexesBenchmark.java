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

package com.hazelcast.query;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.IMap;
import com.hazelcast.internal.util.ThreadLocalRandomProvider;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 1, warmups = 0)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
public class CompositeIndexesBenchmark {

    IMap<Integer, Pojo> map;

    @Setup
    public void setup() {
        Config config = new Config();
        MapConfig mapConfig = config.getMapConfig("map");

        mapConfig.addMapIndexConfig(new MapIndexConfig("f1", false));
        mapConfig.addMapIndexConfig(new MapIndexConfig("f2", false));
        mapConfig.addMapIndexConfig(new MapIndexConfig("f3, f4", false));

        mapConfig.addMapIndexConfig(new MapIndexConfig("f5", false));
        mapConfig.addMapIndexConfig(new MapIndexConfig("f6", true));
        mapConfig.addMapIndexConfig(new MapIndexConfig("f7, f8", true));

        this.map = Hazelcast.newHazelcastInstance(config).getMap("map");
        for (int i = 0; i < 100000; ++i) {
            this.map.put(i, new Pojo(0, i, 0, i, 0, i % 100, 0, i % 100));
        }
    }

    @TearDown
    public void tearDown() {
        Hazelcast.shutdownAll();
    }

    @Benchmark
    public void benchmarkRegularPointQuery() {
        map.values(new SqlPredicate("f1 = 0 and f2 = 1"));
    }

    @Benchmark
    public void benchmarkCompositePointQuery() {
        map.values(new SqlPredicate("f3 = 0 and f4 = 1"));
    }

    @Benchmark
    public void benchmarkRegularRangeQuery() {
        map.values(new SqlPredicate("f5 = 0 and f6 < 1"));
    }

    @Benchmark
    public void benchmarkCompositeRangeQuery() {
        map.values(new SqlPredicate("f7 = 0 and f8 < 1"));
    }

    @Benchmark
    public void mixedLoad_readAndWrite() {
        Random threadLocalRandom = ThreadLocalRandomProvider.get();
        Object readObject;
        for (int i = 0; i < 10000; i++) {
            if (threadLocalRandom.nextInt(100) < 10) {
                readObject = map.get(i);
            } else {
                map.put(i, new Pojo(0, i, 0, i, 0, i % 100, 0, i % 100));
            }
        }
    }

    @Benchmark
    public void mixedLoad_queryAndWrite() {
        Random threadLocalRandom = ThreadLocalRandomProvider.get();
        for (int i = 0; i < 100; i++) {
            if (threadLocalRandom.nextInt(100) < 50) {
                map.values(new SqlPredicate("f1 = 0 and f2 = 1"));
            } else {
                map.put(i, new Pojo(0, i, 0, i, 0, i % 100, 0, i % 100));
            }
        }
    }

    public static class Pojo implements DataSerializable {

        private int f1;
        private int f3;
        private int f2;
        private int f4;

        private int f5;
        private int f6;
        private int f7;
        private int f8;

        public Pojo() {
        }

        public Pojo(int f1, int f2, int f3, int f4, int f5, int f6, int f7, int f8) {
            this.f1 = f1;
            this.f2 = f2;
            this.f3 = f3;
            this.f4 = f4;
            this.f5 = f5;
            this.f6 = f6;
            this.f7 = f7;
            this.f8 = f8;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(f1);
            out.writeInt(f2);
            out.writeInt(f3);
            out.writeInt(f4);
            out.writeInt(f5);
            out.writeInt(f6);
            out.writeInt(f7);
            out.writeInt(f8);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            f1 = in.readInt();
            f2 = in.readInt();
            f3 = in.readInt();
            f4 = in.readInt();
            f5 = in.readInt();
            f6 = in.readInt();
            f7 = in.readInt();
            f8 = in.readInt();
        }

    }

    /*
     * 16 threads, 10% read + 90% write mixed load, query f1 + f2
     *
     * JDK8 Stamped Lock (+ a single optimistic read impl)
     * # Run complete. Total time: 00:04:38
     *
     * Benchmark                                          Mode  Cnt     Score     Error  Units
     * CompositeIndexesBenchmark.mixedLoad_queryAndWrite  avgt   10  5276,369 ± 321,982  ms/op
     * CompositeIndexesBenchmark.mixedLoad_readAndWrite   avgt   10   726,128 ±  25,284  ms/op
     *
     *
     * JDK6 ReentrantReadWriteLock (executed on JDK8)
     * Benchmark                                          Mode  Cnt     Score     Error  Units
     * CompositeIndexesBenchmark.mixedLoad_queryAndWrite  avgt   10  4247,451 ± 507,855  ms/op
     * CompositeIndexesBenchmark.mixedLoad_readAndWrite   avgt   10   922,682 ±  28,159  ms/op
     */

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(CompositeIndexesBenchmark.class.getSimpleName())
                .exclude("benchmark.*")
                .resultFormat(ResultFormatType.JSON)
                .threads(16)
                //                .addProfiler(GCProfiler.class)
                //                .addProfiler(LinuxPerfProfiler.class)
                //                .addProfiler(HotspotMemoryProfiler.class)
                //                .shouldDoGC(true)
                //                .verbosity(VerboseMode.SILENT)
                .build();

        new Runner(opt).run();
    }

}
