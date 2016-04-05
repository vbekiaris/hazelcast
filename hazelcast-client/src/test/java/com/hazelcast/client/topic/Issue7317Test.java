package com.hazelcast.client.topic;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Message;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.topic.ReliableMessageListener;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 *
 */
@Category({QuickTest.class})
public class Issue7317Test {

    static final String smallRB = "foo";
    static final int smallRBCapacity = 3;

    private static HazelcastInstance[] hzs = new HazelcastInstance[3];

    @BeforeClass
    public static void setup() {
        Config conf = new Config();
        RingbufferConfig rbConf = new RingbufferConfig(smallRB);
        rbConf.setCapacity(smallRBCapacity);
        conf.addRingBufferConfig(rbConf);
        for (int i = 0; i < hzs.length; i++) {
            hzs[i] = Hazelcast.newHazelcastInstance(conf);
        }
    }

    @AfterClass
    public static void teardown() {
        for (HazelcastInstance hz : hzs) {
            hz.shutdown();
        }
    }

    @Test
    public void stale() throws InterruptedException {
        ClientConfig conf = new ClientConfig();
        conf.getNetworkConfig().addAddress("localhost");
        HazelcastInstance client = HazelcastClient.newHazelcastClient(conf);
        final List<String> messages = Arrays.asList("a", "b", "c", "d", "e");
        final CountDownLatch cdl = new CountDownLatch(smallRBCapacity);
        ITopic<String> rTopic = client.getReliableTopic(smallRB);
        for (String message : messages) {
            rTopic.publish(message);
        }
        ReliableMessageListener<String> listener = new ReliableMessageListener<String>() {
            long seq;
            public void onMessage(Message<String> msg) {
                System.out.println("Got message" + msg.getMessageObject());
                assertEquals(messages.size() - cdl.getCount(), (int) seq);
                assertEquals(messages.get((int) seq), msg.getMessageObject());
                cdl.countDown();
            }
            public long retrieveInitialSequence() {
                return 0;
            }
            public void storeSequence(long sequence) {
                seq = sequence;
            }
            public boolean isLossTolerant() {
                return true;
            }
            public boolean isTerminal(Throwable failure) {
                return true;
            }
        };
        String reg = rTopic.addMessageListener(listener);
        assertTrue(cdl.await(15, TimeUnit.SECONDS));
        rTopic.removeMessageListener(reg);
    }

    @Test
    public void staleEmbedded() throws InterruptedException {
        Config conf = new Config();
        RingbufferConfig rbConf = new RingbufferConfig(smallRB);
        rbConf.setCapacity(smallRBCapacity);
        conf.addRingBufferConfig(rbConf);
        HazelcastInstance hz = Hazelcast.newHazelcastInstance(conf);
        final List<String> messages = Arrays.asList("a", "b", "c", "d", "e");
        final CountDownLatch cdl = new CountDownLatch(smallRBCapacity);
        ITopic<String> rTopic = hz.getReliableTopic(smallRB);
        for (String message : messages) {
            rTopic.publish(message);
        }
        ReliableMessageListener<String> listener = new ReliableMessageListener<String>() {
            long seq;
                public void onMessage(Message<String> msg) {
                System.out.println("Got message" + msg.getMessageObject());
                assertEquals(messages.size() - cdl.getCount(), (int) seq);
                assertEquals(messages.get((int) seq), msg.getMessageObject());
                cdl.countDown();
            }
            public long retrieveInitialSequence() {
                return 0;
            }
            public void storeSequence(long sequence) {
                seq = sequence;
            }
            public boolean isLossTolerant() {
                return true;
            }
            public boolean isTerminal(Throwable failure) {
                return true;
            }
        };
        String reg = rTopic.addMessageListener(listener);
        assertTrue(cdl.await(5, TimeUnit.SECONDS));
        rTopic.removeMessageListener(reg);
    }
}
