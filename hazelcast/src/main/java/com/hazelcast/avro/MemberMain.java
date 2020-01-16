package com.hazelcast.avro;

import com.hazelcast.avro.impl.GenericRecordStreamSerializer;
import com.hazelcast.avro.impl.ReplicatedMapSchemaRegistry;
import com.hazelcast.config.Config;
import com.hazelcast.config.GlobalSerializerConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.query.SqlPredicate;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

public class MemberMain {

    public static final String USER_SCHEMA = "{\n" + "  \"namespace\": \"com.github.vbekiaris.hzavro.domain\",\n"
            + "  \"type\": \"record\",\n"
            + "  \"name\": \"User\",\n" + "  \"fields\": [\n" + "    {\"name\": \"id\", \"type\": \"int\"},\n"
            + "    {\"name\": \"name\", \"type\": \"string\"}\n" + "  ]\n" + "}";

    public static void main(String[] args)
            throws InterruptedException {
        System.setProperty("hazelcast.avro.schemaRegistryClassName", ReplicatedMapSchemaRegistry.class.getName());
        SerializationConfig serializationConfig = new SerializationConfig();
        // register the GenericRecord serializer as global stream serializer
        GlobalSerializerConfig globalSerializerConfig = new GlobalSerializerConfig();
        globalSerializerConfig.setClassName(GenericRecordStreamSerializer.class.getName());
        serializationConfig.setGlobalSerializerConfig(globalSerializerConfig);

        Config config = new Config();
        config.setSerializationConfig(serializationConfig);

        HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);

        ReplicatedMap<String, String> schemaRegistry = hz.getReplicatedMap(ReplicatedMapSchemaRegistry.AVRO_SCHEMAS_MAP);
        schemaRegistry.put("com.github.vbekiaris.hzavro.domain.User", USER_SCHEMA);
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(USER_SCHEMA);

        IMap<Integer, GenericRecord> records = hz.getMap("users");

        int size = records.size();
        if (size == 0) {
            for (int i = 0; i < 100; i++) {
                GenericRecordBuilder builder = new GenericRecordBuilder(schema);
                builder.set("id", i);
                builder.set("name", "user" + i);
                records.put(i, builder.build());
            }
            System.out.println("Put " + records.size() + " users in the map");
        } else {
            System.out.println("Map already contains " + size + " users");
        }
        System.out.println(records.values(new SqlPredicate("name = 'user2'")));
    }
}
