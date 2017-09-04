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

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class TopicBounceTest extends HazelcastTestSupport {

    private static final ILogger LOGGER = Logger.getLogger(TopicBounceTest.class);

    String topicName;

    @Rule
    public BounceMemberRule bounceMemberRule = BounceMemberRule.with(getConfig())
                                                        .clusterSize(2)
                                                        .driverCount(1)
                                                        .driverType(driverType())
                                                        .build();


    @Test(timeout = 1060000)
    public void testTopic_noMessagesLost() {
        HazelcastInstance testDriver = bounceMemberRule.getNextTestDriver();
        topicName = generateKeyNotOwnedBy(bounceMemberRule.getSteadyMember());
        Producer producer = new Producer(testDriver, topicName);
        ITopic<Integer> topic = testDriver.getTopic(topicName);
        int listenersCount = 10;
        CounterListener[] listeners = new CounterListener[listenersCount];
        for (int i = 0; i < listenersCount; i++) {
            listeners[i] = new CounterListener();
            topic.addMessageListener(listeners[i]);
        }
        bounceMemberRule.testRepeatedly(1, producer, 1000);
        LOGGER.severe("Test concluded - produced " + producer.counter.get() + " values");
    }


    protected BounceTestConfiguration.DriverType driverType() {
        return MEMBER;
    }

    public static class Producer implements Runnable {
        AtomicInteger counter = new AtomicInteger();
        private final HazelcastInstance instance;
        private final String topicName;

        public Producer(HazelcastInstance instance, String topicName) {
            this.topicName = topicName;
            this.instance = instance;
        }

        @Override
        public void run() {
            ITopic<Integer> topic = instance.getTopic(topicName);
            // publish 1, 2, 3, ...
            topic.publish(counter.incrementAndGet());
        }
    }

    public static class CounterListener implements MessageListener<Integer> {
        private AtomicInteger counterTracker = new AtomicInteger();
        @Override
        public void onMessage(Message<Integer> message) {
            int expected = counterTracker.incrementAndGet();
            if (expected != message.getMessageObject()) {
                LOGGER.severe(format("Was expecting %d but received %d", counterTracker.get(), message.getMessageObject()));
                counterTracker.set(message.getMessageObject());
                throw new IllegalStateException(format("Was expecting %d but received %d", expected,
                        message.getMessageObject()));
            }
        }
    }

}
