package com.hazelcast.avro.hzavro.main;

import com.hazelcast.avro.AvroStreamSerializer;
import com.hazelcast.avro.GenericRecordStreamSerializer;
import com.hazelcast.avro.hzavro.domain.User;
import com.hazelcast.avro.impl.PathSchemaRegistry;
import com.hazelcast.avro.impl.ReplicatedMapSchemaRegistry;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.GlobalSerializerConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.ReplicatedMap;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import java.io.IOException;
import java.nio.file.Paths;

import static com.hazelcast.avro.impl.PathSchemaRegistry.AVRO_SCHEMAS_PATH_PROPERTY;
import static com.hazelcast.avro.impl.ReplicatedMapSchemaRegistry.AVRO_SCHEMAS_MAP;

public class Main {

    // this uses the ReplicatedMap as schema registry
    public static void main(String[] args)
            throws InterruptedException {
        System.setProperty(AvroStreamSerializer.AVRO_SCHEMA_REGISTRY_PROPERTY_NAME,
                ReplicatedMapSchemaRegistry.class.getName());
        SerializationConfig serializationConfig = new SerializationConfig();
        // register the GenericRecord serializer
        SerializerConfig genericRecordSerializerConfig = new SerializerConfig();
        genericRecordSerializerConfig.setTypeClass(GenericRecord.class);
        genericRecordSerializerConfig.setClass(GenericRecordStreamSerializer.class);
        serializationConfig.addSerializerConfig(genericRecordSerializerConfig);
        // register the reflect datum global Avro serializer
        GlobalSerializerConfig globalSerializerConfig = new GlobalSerializerConfig();
        globalSerializerConfig.setClassName(AvroStreamSerializer.class.getName());
        serializationConfig.setGlobalSerializerConfig(globalSerializerConfig);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setSerializationConfig(serializationConfig);

        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
//        genericRecords(client);
        users(client);
    }

    // this uses the local path as schema registry
    public static void main2(String[] args) throws InterruptedException {
        System.setProperty(AvroStreamSerializer.AVRO_SCHEMA_REGISTRY_PROPERTY_NAME,
                PathSchemaRegistry.class.getName());
        System.setProperty(AVRO_SCHEMAS_PATH_PROPERTY,
                Paths.get("src/main/avro/user.avsc").toAbsolutePath().normalize().toString());
        SerializationConfig serializationConfig = new SerializationConfig();
        // register the GenericRecord serializer
        SerializerConfig genericRecordSerializerConfig = new SerializerConfig();
        genericRecordSerializerConfig.setTypeClass(GenericRecord.class);
        genericRecordSerializerConfig.setClass(GenericRecordStreamSerializer.class);
        serializationConfig.addSerializerConfig(genericRecordSerializerConfig);
        // register the reflect datum global Avro serializer
        GlobalSerializerConfig globalSerializerConfig = new GlobalSerializerConfig();
        globalSerializerConfig.setClassName(AvroStreamSerializer.class.getName());
        serializationConfig.setGlobalSerializerConfig(globalSerializerConfig);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setSerializationConfig(serializationConfig);

        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        //        genericRecords(client);
        users(client);
    }

    private static void genericRecords(HazelcastInstance client)
            throws IOException, InterruptedException {

        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(Paths.get("src/main/avro/user.avsc").toFile());

        IMap<Integer, GenericRecord> records = client.getMap("records");

        int size = records.size();
        if (size == 0) {
            for (int i = 0; i < 100; i++) {
                GenericRecordBuilder builder = new GenericRecordBuilder(schema);
                builder.set("id", i);
                builder.set("name", "user" + i);
                records.put(i, builder.build());
            }
            System.out.println("Java client put " + records.size() + " users in the map");
        } else {
            System.out.println("Map already contains " + size + " users");
        }

        while (true) {
            Thread.sleep(10000);
            System.out.println("Java client reads: " + records.get(1));
        }
    }

    private static void users(HazelcastInstance client)
            throws InterruptedException {
        ReplicatedMap<String, String> schemaRegistry = client.getReplicatedMap(AVRO_SCHEMAS_MAP);
        schemaRegistry.put("com.github.vbekiaris.hzavro.domain.User",
                "{\n" + "  \"namespace\": \"com.github.vbekiaris.hzavro.domain\",\n" + "  \"type\": \"record\",\n"
                        + "  \"name\": \"User\",\n" + "  \"fields\": [\n" + "    {\"name\": \"id\", \"type\": \"int\"},\n"
                        + "    {\"name\": \"name\", \"type\": \"string\"}\n" + "  ]\n" + "}");
        IMap<Integer, User> users = client.getMap("users");

        int size = users.size();
        if (size == 0) {
            for (int i = 0; i < 100; i++) {
                users.put(i, new User(i, "user" + i, "lastName" + i));
            }
            System.out.println("Java client put " + users.size() + " users in the map");
        } else {
            System.out.println("Map already contains " + size + " users");
        }

        while (true) {
            Thread.sleep(10000);
            System.out.println("Java client reads: " + users.get(1));
        }
    }
}
