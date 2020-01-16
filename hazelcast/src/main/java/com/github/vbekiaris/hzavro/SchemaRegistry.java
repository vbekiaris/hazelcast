package com.github.vbekiaris.hzavro;

import org.apache.avro.Schema;

public interface SchemaRegistry {

    Schema getSchema(int schemaId);

    Schema getSchema(String typeName);

    Schema getSchema(String typeName, int version);

    void destroy();
}
