package com.hazelcast.avro.hzavro;

import com.hazelcast.avro.hzavro.domain.Session;
import com.hazelcast.avro.hzavro.domain.User;
import com.hazelcast.avro.impl.ReplicatedMapSchemaRegistry;
import com.hazelcast.config.Config;
import com.hazelcast.config.GlobalSerializerConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static com.hazelcast.avro.impl.ReplicatedMapSchemaRegistry.AVRO_SCHEMAS_MAP;
import static org.junit.Assert.assertEquals;

public class AvroSerializerIntegrationTest {

    private static final String USER_SCHEMA = "{\n" + "  \"namespace\": \"com.github.vbekiaris.hzavro.domain\",\n"
            + "  \"type\": \"record\",\n"
            + "  \"name\": \"User\",\n" + "  \"fields\": [\n" + "    {\"name\": \"id\", \"type\": \"int\"},\n"
            + "    {\"name\": \"name\", \"type\": \"string\"}\n" + "  ]\n" + "}";

    private static final String SESSION_SCHEMA = "{\n" + "  \"namespace\": \"com.github.vbekiaris.hzavro.domain\",\n"
            + "  \"type\": \"record\",\n"
            + "  \"name\": \"Session\",\n" + "  \"fields\": [\n" + "    {\"name\": \"createdOn\", \"type\": \"long\"},\n"
            + "    {\"name\": \"expiresOn\", \"type\": \"long\"}\n" + "  ]\n" + "}";



    private HazelcastInstance member;
    private ReplicatedMap<String, String> schemaRegistry;

    // start a member
    @Before
    public void setup()
            throws Exception {
        System.setProperty(AvroStreamSerializer.AVRO_SCHEMA_REGISTRY_PROPERTY_NAME, ReplicatedMapSchemaRegistry.class.getName());
        SerializationConfig serializationConfig = new SerializationConfig();
        GlobalSerializerConfig globalSerializerConfig = new GlobalSerializerConfig();
        globalSerializerConfig.setImplementation(new AvroStreamSerializer());
        serializationConfig.setGlobalSerializerConfig(globalSerializerConfig);
        Config config = new Config();
        config.setSerializationConfig(serializationConfig);
        member = Hazelcast.newHazelcastInstance(config);
        schemaRegistry = member.getReplicatedMap(AVRO_SCHEMAS_MAP);
    }

    @After
    public void tearDown() {
        System.clearProperty(AvroStreamSerializer.AVRO_SCHEMA_REGISTRY_PROPERTY_NAME);
    }

    @Test
    public void test() {
        schemaRegistry.put("com.github.vbekiaris.hzavro.domain.User", USER_SCHEMA);
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
        assertEquals("but I am 2", map.get(1).getName());
    }

    @Test(expected = HazelcastSerializationException.class)
    public void entryProcessor_failsWhenUnknownSchema() {
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
        assertEquals("but I am 2", map.get(1));
    }

    @Test
    public void entryProcessor_worksWhenSchemaAddedDynamically()
            throws InterruptedException {
        schemaRegistry.put("com.github.vbekiaris.hzavro.domain.User", USER_SCHEMA);
        IMap<Integer, User> map = member.getMap("users");
        map.put(1, new User(1, "1"));
        // add the schema
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

        schemaRegistry.put("com.github.vbekiaris.hzavro.domain.Session", SESSION_SCHEMA);
        Thread.sleep(3000);
        IMap<Integer, Session> sessions = member.getMap("sessions");
        sessions.put(1, new Session(1L, 2L));

        sessions.submitToKey(1, new EntryProcessor() {
            @Override
            public Object process(Map.Entry entry) {
                entry.setValue(new Session(1L, 5L));
                return null;
            }

            @Override
            public EntryBackupProcessor getBackupProcessor() {
                return null;
            }
        });

        assertEquals(5L, sessions.get(1).getExpiresOn());
    }
}
