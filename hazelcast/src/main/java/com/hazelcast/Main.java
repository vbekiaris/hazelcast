package com.hazelcast;

import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.FileSystemXmlConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.util.Natives;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 */
public class Main {

    static final Pattern pattern = Pattern.compile("n(127\\.0\\.0\\.\\d*):(\\d*)->(127\\.0\\.0\\.\\d*):(\\d*)");

    static List<Integer> allowedClientPorts = Arrays.asList(new Integer[] {5701, 10000, 10001, 10002, 10003, 10004});

    public static void main(String[] args)
            throws IOException, InterruptedException {

        int pid = Natives.INSTANCE.getpid();
        System.setProperty("hazelcast.socket.bind.any", "false");
        HazelcastInstance[] hzs = new HazelcastInstance[4];
        for (int i = 0; i < hzs.length; i++) {
            Config config = new ClasspathXmlConfig("hazelcast-outboundports.xml");
            config.getNetworkConfig().getInterfaces().setEnabled(true);
            config.getNetworkConfig().getInterfaces().addInterface("127.0.0." + (i + 1));
            hzs[i] = Hazelcast.newHazelcastInstance(config);
        }
        try {
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
            for (HazelcastInstance hz : hzs) {
                hz.shutdown();
            }
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
