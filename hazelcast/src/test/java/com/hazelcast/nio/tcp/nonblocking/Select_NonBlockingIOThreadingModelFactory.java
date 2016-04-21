package com.hazelcast.nio.tcp.nonblocking;

import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.nio.tcp.IOThreadingModelFactory;
import com.hazelcast.nio.tcp.MockIOService;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.spi.properties.HazelcastProperties;

import java.util.Properties;

public class Select_NonBlockingIOThreadingModelFactory implements IOThreadingModelFactory {

    @Override
    public NonBlockingIOThreadingModel create(
            MockIOService ioService, MetricsRegistry metricsRegistry) {
        Properties properties = new Properties();
        properties.put(GroupProperty.IO_INPUT_THREAD_SELECT_NOW.getName(), "false");
        properties.put(GroupProperty.IO_OUTPUT_THREAD_SELECT_NOW.getName(), "false");
        NonBlockingIOThreadingModel threadingModel = new NonBlockingIOThreadingModel(
                ioService,
                ioService.loggingService,
                metricsRegistry,
                ioService.hazelcastThreadGroup,
                new HazelcastProperties(new Properties()));
        return threadingModel;
    }
}
