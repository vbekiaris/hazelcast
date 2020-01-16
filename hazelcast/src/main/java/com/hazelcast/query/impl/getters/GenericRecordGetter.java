package com.hazelcast.query.impl.getters;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

final class GenericRecordGetter extends Getter {

    public static final GenericRecordGetter INSTANCE = new GenericRecordGetter();

    public GenericRecordGetter() {
        super(null);
    }

    @Override
    Object getValue(Object obj) throws Exception {
        throw new IllegalArgumentException("GenericRecordGetter does not read without field path");
    }

    @Override
    Object getValue(Object target, String fieldPath) throws Exception {
        GenericRecord record = (GenericRecord) target;
        Object value = record.get(fieldPath);
        if (value instanceof Utf8) {
            value = value.toString();
        }
        return value;
    }

    @Override
    Class getReturnType() {
        throw new IllegalArgumentException("GenericRecordGetter does not return a specific type");
    }

    @Override
    boolean isCacheable() {
        return false;
    }

}
