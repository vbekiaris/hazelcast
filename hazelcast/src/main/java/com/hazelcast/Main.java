package com.hazelcast;

import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.util.Natives;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 */
public class Main {

    static final Pattern pattern = Pattern.compile("n(127\\.0\\.0\\.\\d*):(\\d*)->(127\\.0\\.0\\.\\d*):(\\d*)");

    static List<Integer> allowedClientPorts = Arrays.asList(new Integer[] {5701, 10000, 10001, 10002, 10003, 10004});

    static final int CLUSTER_SIZE = 4;

    static HazelcastInstance[] HZS = new HazelcastInstance[CLUSTER_SIZE];

    static final CyclicBarrier BARRIER = new CyclicBarrier(CLUSTER_SIZE+1);

    static final ExecutorService EXECUTOR_SERVICE = Executors.newFixedThreadPool(CLUSTER_SIZE);

    static class StartHazelcastTask implements Callable<Void> {
        final int sequence;

        StartHazelcastTask(int sequence) {
            this.sequence = sequence;
        }

        @Override
        public Void call()
                throws InterruptedException, BrokenBarrierException {
            BARRIER.await();
            Config config = new ClasspathXmlConfig("hazelcast-outboundports.xml");
            config.getNetworkConfig().getInterfaces().setEnabled(true);
            config.getNetworkConfig().getInterfaces().addInterface("127.0.0." + (sequence + 1));
            HZS[sequence] = Hazelcast.newHazelcastInstance(config);
            BARRIER.await();
            return null;
        }
    }

    public static void main(String[] args)
            throws IOException, InterruptedException, BrokenBarrierException {

        int pid = Natives.INSTANCE.getpid();
        System.setProperty("hazelcast.socket.bind.any", "false");

        for (int i = 0; i < CLUSTER_SIZE; i++) {
            EXECUTOR_SERVICE.submit(new StartHazelcastTask(i));
        }
        try {
            BARRIER.await();
            BARRIER.await();

            Process p = Runtime.getRuntime().exec(new String[]{"/usr/sbin/lsof", "-F", "n", "-a",
                                                   "-n", "-P", "-i", "-p", "" + pid});
            p.waitFor();
            InputStream lsofStdout = p.getInputStream();
            String lsofOutput = new Scanner(lsofStdout).useDelimiter("\\A").next();
            List<Integer> clientPorts = clientPorts(lsofOutput);

            for (Integer i : clientPorts) {
                if (!allowedClientPorts.contains(i)) {
                    throw new IllegalStateException(
                            "Opened outbound port outside allowed range: " + i + ". Complete lsof output: \n" + lsofOutput);
                }
            }
        }
        finally {
            for (HazelcastInstance hz : HZS) {
                hz.shutdown();
            }
            EXECUTOR_SERVICE.shutdown();
        }
    }

    private static List<Integer> clientPorts(String lsofOutput) {
        List<Integer> clientPorts = new ArrayList<Integer>();
        for (String line : lsofOutput.split("\n")) {
            Matcher m = pattern.matcher(line);
            if (m.matches()) {
                // CLIENT(IP:PORT)->SERVER(IP:PORT)
                clientPorts.add(Integer.valueOf(m.group(2)));
            }
        }
        return clientPorts;
    }
}
