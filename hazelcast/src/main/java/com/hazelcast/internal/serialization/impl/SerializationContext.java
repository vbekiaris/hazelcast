package com.hazelcast.internal.serialization.impl;

public interface SerializationContext {

    int getContextId();

    SerializerAdapter getSerializerFor(Class<?> klass);

    SerializerAdapter getSerializerFor(int typeId);

    boolean register(Class<?> klass, SerializerAdapter serializerAdapter);

    boolean unregister(Class<?> klass);

    void destroy();
}
