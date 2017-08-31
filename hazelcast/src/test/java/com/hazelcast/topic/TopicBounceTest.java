/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.topic;

import com.hazelcast.config.Config;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.TopicConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.test.bounce.BounceMemberRule;
import com.hazelcast.test.bounce.BounceTestConfiguration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.test.bounce.BounceTestConfiguration.DriverType.MEMBER;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class TopicBounceTest extends HazelcastTestSupport {

    private static final ILogger LOGGER = Logger.getLogger(TopicBounceTest.class);

    String topicName = randomName();

    @Rule
    public BounceMemberRule bounceMemberRule = BounceMemberRule.with(getConfig())
                                                        .clusterSize(2)
                                                        .driverCount(1)
                                                        .driverType(driverType())
                                                        .useTerminate()
                                                        .build();


    @Test
    public void testTopic_noMessagesLost() {
        HazelcastInstance testDriver = bounceMemberRule.getNextTestDriver();
        Producer producer = new Producer(testDriver, topicName);
        ITopic<Integer> topic = testDriver.getTopic(topicName);
        int listenersCount = 10;
        CounterListener[] listeners = new CounterListener[listenersCount];
        for (int i = 0; i < listenersCount; i++) {
            listeners[i] = new CounterListener();
            topic.addMessageListener(listeners[i]);
        }
        bounceMemberRule.testRepeatedly(1, producer, 60);
        LOGGER.severe("Test concluded - produced " + producer.counter.get() + " values");
        for (CounterListener listener : listeners) {
            assertEquals(producer.counter.get(), listener.counterTracker.get());
        }
    }


    protected BounceTestConfiguration.DriverType driverType() {
        return MEMBER;
    }

    @Override
    protected Config getConfig() {
        Config config = super.getConfig();
        config.setProperty("hazelcast.event.queue.capacity", "20");
        config.setProperty("hazelcast.event.queue.timeout.millis", "20");
        TopicConfig topicConfig = new TopicConfig(topicName);
        topicConfig.addMessageListenerConfig(new ListenerConfig(new CounterListener()));
        return config;
    }

    public static class Producer implements Runnable {
        AtomicInteger counter = new AtomicInteger();
        private final String topicName;
        private final ITopic<Integer> topic;

        public Producer(HazelcastInstance instance, String topicName) {
            this.topicName = topicName;
            this.topic = instance.getTopic(topicName);
        }

        @Override
        public void run() {
            // publish 1, 2, 3, ...
            topic.publish(counter.incrementAndGet());
        }
    }

    public static class CounterListener implements MessageListener<Integer> {
        private AtomicInteger counterTracker = new AtomicInteger();
        @Override
        public void onMessage(Message<Integer> message) {
            int expected = counterTracker.incrementAndGet();
            if (expected != message.getMessageObject().intValue()) {
                LOGGER.severe(format("Was expecting %d but received %d", expected, message.getMessageObject()));
                counterTracker.set(message.getMessageObject());
                throw new IllegalStateException(format("Was expecting %d but received %d", expected,
                        message.getMessageObject()));
            }
        }
    }

}
