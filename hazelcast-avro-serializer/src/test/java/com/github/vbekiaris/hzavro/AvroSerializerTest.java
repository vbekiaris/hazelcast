package com.github.vbekiaris.hzavro;

import com.github.vbekiaris.hzavro.domain.User;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.ObjectDataInputStream;
import com.hazelcast.internal.serialization.impl.ObjectDataOutputStream;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static com.github.vbekiaris.hzavro.impl.PathSchemaRegistry.AVRO_SCHEMAS_PATH_PROPERTY;
import static org.junit.Assert.assertEquals;

public class AvroSerializerTest {

    private InternalSerializationService serializationService;
    private AvroStreamSerializer avroSerializer;
    private ObjectDataOutput output;
    private File file;
    private ObjectDataInput input;

    @Before
    public void setup()
            throws Exception {
        System.setProperty(AVRO_SCHEMAS_PATH_PROPERTY, "/Users/vb/tmp/avro");
        serializationService = new DefaultSerializationServiceBuilder().build();
        avroSerializer = new AvroStreamSerializer();
        Path tmpOut = Paths.get("/Users/vb/workspace/github/vbekiaris/hazelcast-avro/test.avro");
        Files.deleteIfExists(tmpOut);
        file = Files.createFile(tmpOut).toFile();
        output = new ObjectDataOutputStream(new FileOutputStream(file), serializationService);
    }

    @After
    public void tearDown() {
        System.clearProperty(AVRO_SCHEMAS_PATH_PROPERTY);
    }

    @Test
    public void test_serialize()
            throws IOException {
        User user = new User(3, "vassilis");
        avroSerializer.write(output, user);
        input = new ObjectDataInputStream(new FileInputStream(file), serializationService);
        User deserialized = (User) avroSerializer.read(input);
        assertEquals(user, deserialized);
    }

}
