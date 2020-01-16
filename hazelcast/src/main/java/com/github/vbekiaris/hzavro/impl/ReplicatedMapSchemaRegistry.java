package com.github.vbekiaris.hzavro.impl;

import com.github.vbekiaris.hzavro.SchemaRegistry;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.MapEvent;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import org.apache.avro.Schema;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class ReplicatedMapSchemaRegistry
        implements SchemaRegistry, HazelcastInstanceAware, EntryListener<String, String> {

    public static final String AVRO_SCHEMAS_MAP = "hz:schemas";

    private final AtomicBoolean initialized = new AtomicBoolean();

    // type name -> JSON schema
    private volatile ReplicatedMap<String, String> imapSchemas;
    private volatile HazelcastInstance hz;

    private volatile ConcurrentMap<Integer, Schema> schemasById;
    private volatile ConcurrentMap<String, Schema[]> schemasByTypeName;
    private final ILogger logger;

    public ReplicatedMapSchemaRegistry() {
        logger = Logger.getLogger(ReplicatedMapSchemaRegistry.class);
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        hz = hazelcastInstance;
    }

    @Override
    public Schema getSchema(int schemaId) {
        initialize();
        synchronized (this) {
            return schemasById.get(schemaId);
        }
    }

    @Override
    public Schema getSchema(String typeName) {
        initialize();
        synchronized (this) {
            return schemasByTypeName.get(typeName)[0];
        }
    }

    @Override
    public Schema getSchema(String typeName, int version) {
        initialize();
        synchronized (this) {
            return schemasByTypeName.get(typeName)[0];
        }
    }

    @Override
    public void destroy() {

    }

    @Override
    public void entryAdded(EntryEvent<String, String> event) {
        scanSchemas();
    }

    @Override
    public void entryEvicted(EntryEvent<String, String> event) {
        scanSchemas();
    }

    @Override
    public void entryRemoved(EntryEvent<String, String> event) {
        scanSchemas();
    }

    @Override
    public void entryUpdated(EntryEvent<String, String> event) {
        scanSchemas();
    }

    @Override
    public void mapCleared(MapEvent event) {
        scanSchemas();
    }

    @Override
    public void mapEvicted(MapEvent event) {
        scanSchemas();
    }

    private void initialize() {
        if (initialized.compareAndSet(false, true)) {
            imapSchemas = hz.getReplicatedMap(AVRO_SCHEMAS_MAP);
            scanSchemas();
            registerListener();
        }
    }

    private void registerListener() {
        imapSchemas.addEntryListener(this);
    }

    private void scanSchemas() {
        ConcurrentMap<Integer, Schema> schemas = new ConcurrentHashMap<>();
        ConcurrentMap<String, Schema[]> schemasByTypeName = new ConcurrentHashMap<>();
        final Map<String, String> sourceSchemas = new HashMap<>(imapSchemas);
        Schema.Parser parser = new Schema.Parser();
        for (Map.Entry<String, String> entry : sourceSchemas.entrySet()) {
            try {
                Schema schema = parser.parse(entry.getValue());
                schemas.put(schema.hashCode(), schema);
                schemasByTypeName.put(schema.getFullName(), new Schema[]{schema});
            } catch (Exception e) {
                // log
                logger.warning(
                        "Schema could not be parsed, probably not an Avro schema? "
                                + "Full schema follows:\n" + entry.getValue(), e);
            }
        }
        synchronized (this) {
            this.schemasById = schemas;
            this.schemasByTypeName = schemasByTypeName;
        }
        dumpKnownSchemas();
    }

    void dumpKnownSchemas() {
        logger.info("Avro serializer scanned and found known schemas: ");
        for (Map.Entry<Integer, Schema> entry : schemasById.entrySet()) {
            logger.info(entry.getKey() + " [ " + entry.getValue().getFullName() + " ]");
        }
    }
}
