package com.hazelcast.test;

import com.hazelcast.config.Config;
import com.hazelcast.config.DiscoveryStrategyConfig;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.ServerSocketEndpointConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.properties.ClusterProperty;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.io.IOException;
import java.util.Map;

/**
 * Unix sockets test
 */
public class AtomicLongApp {

    @State(Scope.Benchmark)
    public static class ExecutionPlan {

        public HazelcastInstance hz;
        IAtomicLong atomicLong;

        @Setup(Level.Trial)
        public void setUp() throws IOException {
            boolean isUnixSocketSupported = true;
            try {
                Class.forName("com.hazelcast.spi.discovery.uds.UDSDiscoveryStrategy");
            } catch (Exception e) {
                isUnixSocketSupported = false;
            }

            Config liteConfig = createConfig(isUnixSocketSupported).setLiteMember(true);
            hz = Hazelcast.newHazelcastInstance(liteConfig);
        }

        protected Config createConfig(boolean isUnixSocketSupported) {
            Config config = new Config();
            config.getMetricsConfig().setEnabled(false);
            config.getAdvancedNetworkConfig().setEnabled(true);
            // issue with advanced networking + no ClIENT endpoint
            config.getAdvancedNetworkConfig().setClientEndpointConfig(new ServerSocketEndpointConfig().setPort(5555));
            JoinConfig joinConfig = config.getAdvancedNetworkConfig().getJoin();
            if (isUnixSocketSupported) {
                joinConfig.getDiscoveryConfig().addDiscoveryStrategyConfig(
                        new DiscoveryStrategyConfig("com.hazelcast.spi.discovery.uds.UDSDiscoveryStrategy",
                                Map.of("hazelcast.disco.udsDirectory", System.getProperty("hazelcast.unix.socket.dir","/mnt"))));
                config.setProperty(ClusterProperty.DISCOVERY_SPI_ENABLED.getName(), "true");
            } else {
                joinConfig.getAutoDetectionConfig().setEnabled(false);
                joinConfig.getMulticastConfig().setEnabled(false);
                joinConfig.getTcpIpConfig().setEnabled(true).addMember("172.17.0.2:5701");
            }
            return config;
        }

        @Setup(Level.Iteration)
        public void setUpIt() {
            atomicLong = hz.getCPSubsystem().getAtomicLong("test");
        }
    }

    @Benchmark
    public long get(ExecutionPlan ep) {
        return ep.atomicLong.get();
    }

    @Benchmark
    public long getAndIncrement(ExecutionPlan ep) {
        return ep.atomicLong.getAndIncrement();
    }
}
