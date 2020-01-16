package com.github.vbekiaris.hzavro;

import com.github.vbekiaris.hzavro.domain.User;
import com.github.vbekiaris.hzavro.impl.PathSchemaRegistry;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.GlobalSerializerConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.IMap;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class AvroSerializerIntegrationTest {

    @Before
    public void setup() {
        System.setProperty(PathSchemaRegistry.AVRO_SCHEMAS_PATH_PROPERTY, "/Users/vb/tmp/avro");
    }

    @After
    public void tearDown() {
        System.clearProperty(PathSchemaRegistry.AVRO_SCHEMAS_PATH_PROPERTY);
    }

    @Test
    public void test()
            throws Exception {
        SerializationConfig serializationConfig = new SerializationConfig();
        GlobalSerializerConfig globalSerializerConfig = new GlobalSerializerConfig();
        globalSerializerConfig.setImplementation(new AvroStreamSerializer());
        serializationConfig.setGlobalSerializerConfig(globalSerializerConfig);
        Config config = new Config();
        config.setSerializationConfig(serializationConfig);
        HazelcastInstance member = Hazelcast.newHazelcastInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setSerializationConfig(serializationConfig);
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

        IMap<Integer, User> clientMap = client.getMap("users");
        clientMap.put(1, new User(1, "1"));
        clientMap.submitToKey(1, (EntryProcessor<Integer, User, Void>) entry -> {
            entry.setValue(new User(1, "but I am 2"));
            return null;
        });
        System.out.println(clientMap.get(1));
    }
}
