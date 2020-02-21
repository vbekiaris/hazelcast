package com.hazelcast.internal.serialization.impl;

import com.hazelcast.internal.util.collection.Int2ObjectHashMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ConcurrentSerializationContext implements SerializationContext {

    private final int contextId;
    private final ConcurrentMap<Class<?>, SerializerAdapter> typeMap;
    private final Int2ObjectHashMap<SerializerAdapter> idMap;

    public ConcurrentSerializationContext(int contextId) {
        this.contextId = contextId;
        this.typeMap = new ConcurrentHashMap<>();
        this.idMap = new Int2ObjectHashMap<>();
    }

    @Override
    public int getContextId() {
        return contextId;
    }

    @Override
    public SerializerAdapter getSerializerFor(Class<?> klass) {
        return typeMap.get(klass);
    }

    @Override
    public SerializerAdapter getSerializerFor(int typeId) {
        return idMap.get(typeId);
    }

    @Override
    public boolean register(@Nullable Class<?> klass,
                            @Nonnull SerializerAdapter serializerAdapter) {
        SerializerAdapter current;
        if (klass != null) {
            current = typeMap.putIfAbsent(klass, serializerAdapter);
            if (current != null && current.getImpl().getClass() != serializerAdapter.getImpl().getClass()) {
                throw new IllegalStateException("Serializer[" + current.getImpl() + "] has been already registered for type: "
                        + klass);
            }
        }
        current = idMap.putIfAbsent(serializerAdapter.getTypeId(), serializerAdapter);
        if (current != null && current.getImpl().getClass() != serializerAdapter.getImpl().getClass()) {
            throw new IllegalStateException(
                    "Serializer [" + current.getImpl() + "] has been already registered for type ID: "
                            + serializerAdapter.getTypeId());
        }
        return current == null;
    }

    @Override
    public boolean unregister(Class<?> klass) {
        SerializerAdapter adapter = typeMap.remove(klass);
        if (adapter != null) {
            idMap.remove(adapter.getTypeId());
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void destroy() {
        for (SerializerAdapter serializer : typeMap.values()) {
            serializer.destroy();
        }
        typeMap.clear();
        idMap.clear();
    }
}
