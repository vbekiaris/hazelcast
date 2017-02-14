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

package com.hazelcast.topic.impl.reliable;

import com.hazelcast.config.Config;
import com.hazelcast.config.ReliableTopicConfig;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.TimeConstants;
import com.hazelcast.test.annotation.Repeat;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.test.bounce.BounceMemberRule;
import com.hazelcast.test.bounce.MemberDriverFactory;
import com.hazelcast.topic.TopicOverloadPolicy;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.test.HazelcastTestSupport.assertEqualsEventually;
import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static com.hazelcast.test.HazelcastTestSupport.sleepSeconds;
import static com.hazelcast.test.bounce.BounceTestConfiguration.DriverType.MEMBER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 *
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class ReliableTopicBounceTest {
    private static final int MESSAGE_COUNT = 10000;
    private static final int LISTENERS_COUNT = 30;

    private String[] topicNames = new String[] {
            "topic" + randomString(),
            "topic" + randomString(),
            "topic" + randomString(),
            "topic" + randomString(),
            "topic" + randomString(),
            "topic" + randomString(),
            };

    @Rule
    public BounceMemberRule bounceMemberRule = initializeBounceTest();

    protected BounceMemberRule initializeBounceTest() {
        return BounceMemberRule.with(config())
                               .driverType(MEMBER)
                               .driverCount(3)
                               .driverFactory(new MemberDriverFactory() {
                                   @Override
                                   protected Config getConfig() {
                                       return config();
                                   }
                               })
                               .build();
    }

    @Test(timeout = 10 * TimeConstants.MINUTE)
    public void test_singleProducer_multipleListeners() {
        // register driver-side message listeners
        System.out.println(">>>>>>>>>>>>>>>> TESTEST");
        final MessageListener[] listeners = new MessageListener[LISTENERS_COUNT];
        for (int i = 0; i < LISTENERS_COUNT; i++) {
            HazelcastInstance hz = bounceMemberRule.getNextTestDriver();
            for (String topicName : topicNames) {
                final ITopic topic = hz.getReliableTopic(topicName);
                listeners[i] = new NonThreadSafeCounter();
                topic.addMessageListener(listeners[i]);
            }
        }

        System.out.println(">>>>>>>>>>>>>>>> PUBLISH");
        // run once-off the producer runnable
        bounceMemberRule.test(new Runnable[] {
                new Runnable() {
                    @Override
                    public void run() {
                        HazelcastInstance hz = bounceMemberRule.getNextTestDriver();
                        for (String topicName : topicNames) {
                            ITopic topic = hz.getReliableTopic(topicName);
                            IAtomicLong publisherCounter = hz.getAtomicLong(topicName + "-publisher");
                            byte[] payload = new byte[]{0};
                            for (int i = 0; i < MESSAGE_COUNT; i++) {
                                topic.publish(payload);
                                publisherCounter.incrementAndGet();
                                Thread.currentThread().yield();
                            }
                        }
                    }
                }
        });

        System.out.println(new Date() + " - Publisher task is done.");
        for (String topicName : topicNames) {
            IAtomicLong publisherCounter = bounceMemberRule.getSteadyMember().getAtomicLong(topicName + "-publisher");
            long publisherCount = publisherCounter.get();
            assertEquals(MESSAGE_COUNT, publisherCount);
        }
        sleepSeconds(30);
        System.out.println(new Date() + " - Done sleeping.");
        for (final MessageListener listener : listeners) {
            long assertDeadline = System.currentTimeMillis() + (3 * TimeConstants.MINUTE);
            long count = ((NonThreadSafeCounter)listener).getCount();
            while (count != MESSAGE_COUNT && (System.currentTimeMillis() < assertDeadline)) {
                System.out.println(new Date() + " - Listener " + listener + " counted " + count + " messages");
                sleepSeconds(3);
                count = ((NonThreadSafeCounter)listener).getCount();
            }
            if (count != MESSAGE_COUNT) {
                fail(listener + " only received " + count + " messages.");
            }
        }
    }

    protected Config config() {
        Config config = new Config();

        RingbufferConfig ringbufferConfig = new RingbufferConfig("topic*");
        ringbufferConfig.setCapacity(2000);
        ringbufferConfig.setTimeToLiveSeconds(60);
        config.addRingBufferConfig(ringbufferConfig);

        ReliableTopicConfig topicConfig = new ReliableTopicConfig("topic*");
        topicConfig.setTopicOverloadPolicy(TopicOverloadPolicy.BLOCK);
        config.addReliableTopicConfig(topicConfig);

        return config;
    }

    public static class NonThreadSafeCounter implements MessageListener {
        private int counter;

        public NonThreadSafeCounter() {
        }

        @Override
        public void onMessage(Message message) {
            counter++;
        }

        public int getCount() {
            return counter;
        }
    }
}
