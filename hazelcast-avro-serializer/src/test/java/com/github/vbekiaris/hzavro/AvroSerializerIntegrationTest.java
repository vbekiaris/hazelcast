package com.github.vbekiaris.hzavro;

import com.github.vbekiaris.hzavro.domain.User;
import com.github.vbekiaris.hzavro.impl.ReplicatedMapSchemaRegistry;
import com.hazelcast.config.Config;
import com.hazelcast.config.GlobalSerializerConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static com.github.vbekiaris.hzavro.impl.ReplicatedMapSchemaRegistry.AVRO_SCHEMAS_MAP;

public class AvroSerializerIntegrationTest {

    @Before
    public void setup() {
        System.setProperty(AvroStreamSerializer.AVRO_SCHEMA_REGISTRY_PROPERTY_NAME, ReplicatedMapSchemaRegistry.class.getName());
    }

    @After
    public void tearDown() {
        System.clearProperty(AvroStreamSerializer.AVRO_SCHEMA_REGISTRY_PROPERTY_NAME);
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

        ReplicatedMap<String, String> schemaRegistry = member.getReplicatedMap(AVRO_SCHEMAS_MAP);
        schemaRegistry.put("com.github.vbekiaris.hzavro.domain.User",
                "{\n" + "  \"namespace\": \"com.github.vbekiaris.hzavro.domain\",\n" + "  \"type\": \"record\",\n"
                        + "  \"name\": \"User\",\n" + "  \"fields\": [\n" + "    {\"name\": \"id\", \"type\": \"int\"},\n"
                        + "    {\"name\": \"name\", \"type\": \"string\"}\n" + "  ]\n" + "}");

        IMap<Integer, User> map = member.getMap("users");
        map.put(1, new User(1, "1"));
        map.submitToKey(1, new EntryProcessor() {
                    @Override
                    public Object process(Map.Entry entry) {
                        entry.setValue(new User(1, "but I am 2"));
                        return null;
                    }

                    @Override
                    public EntryBackupProcessor getBackupProcessor() {
                        return null;
                    }
                });
        System.out.println(map.get(1));
    }
}
