package com.github.vbekiaris.hzavro;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class AvroStreamSerializer implements StreamSerializer, HazelcastInstanceAware {

    public static final String AVRO_SCHEMA_REGISTRY_PROPERTY_NAME = "hazelcast.avro.schemaRegistryClassName";

    private static final String AVRO_SCHEMA_REGISTRY_CLASS_NAME =
            System.getProperty(AVRO_SCHEMA_REGISTRY_PROPERTY_NAME,
                    "com.github.vbekiaris.hzavro.impl.PathSchemaRegistry");

    private static final int AVRO_SERIALIZER_TYPE_ID = Integer.getInteger("hazelcast.avro.serializerTypeId", 1000);
    private final SchemaRegistry schemaRegistry;

    public AvroStreamSerializer() throws Exception {
        schemaRegistry = ClassLoaderUtil.newInstance(null, AVRO_SCHEMA_REGISTRY_CLASS_NAME);
        if (schemaRegistry == null) {
            throw new IllegalStateException("AvroStreamSerializer requires a schema registry implementation. "
                    + "Tried " + AVRO_SCHEMA_REGISTRY_CLASS_NAME + " but new instance was null.");
        }
    }

    @Override
    public void write(ObjectDataOutput out, Object object)
            throws IOException {
        // todo: can i reuse the same encoder instance?
        Encoder encoder = EncoderFactory.get().binaryEncoder((OutputStream) out, null);
        if (object instanceof GenericRecord) {
            GenericRecord record = (GenericRecord) object;
            writeGenericRecord(out, encoder, record);
        } else {
            writeSpecificRecord(out, encoder, object);
        }
    }

    private void writeGenericRecord(ObjectDataOutput out,
                                    Encoder encoder,
                                    GenericRecord record)
            throws IOException {
        DatumWriter writer = GenericData.get().createDatumWriter(record.getSchema());
        // output schema name and the binary blob
        out.writeUTF(record.getSchema().getFullName());
        writer.write(record, encoder);
        encoder.flush();
    }

    private void writeSpecificRecord(ObjectDataOutput out,
                                     Encoder encoder,
                                     Object object)
            throws IOException {
        String typeName = object.getClass().getName();
        Schema schema = schemaRegistry.getSchema(typeName);
        // todo don't create new datum writer each time?
        DatumWriter writer = new ReflectDatumWriter(schema);
        out.writeInt(schema.hashCode());
        writer.write(object, encoder);
        encoder.flush();
    }

    @Override
    public Object read(ObjectDataInput in)
            throws IOException {
        int schemaId = in.readInt();
        Schema schema = schemaRegistry.getSchema(schemaId);
        DatumReader reader = new ReflectDatumReader(schema);
        // todo reuse Decoder instances ?
        Decoder decoder = DecoderFactory.get().binaryDecoder((InputStream) in, null);
        Object deserialized = reader.read(null, decoder);
        return deserialized;
    }

    @Override
    public int getTypeId() {
        return AVRO_SERIALIZER_TYPE_ID;
    }

    @Override
    public void destroy() {

    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        if (schemaRegistry instanceof HazelcastInstanceAware) {
            ((HazelcastInstanceAware) schemaRegistry).setHazelcastInstance(hazelcastInstance);
        }
    }
}
