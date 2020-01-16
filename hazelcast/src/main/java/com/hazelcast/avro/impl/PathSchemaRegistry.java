package com.hazelcast.avro.impl;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import org.apache.avro.Schema;

import java.io.IOException;
import java.nio.file.ClosedWatchServiceException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Stream;

/**
 * A {@link AvroSchemaRegistry} that reads avro schema definitions from a directory
 */
public class PathSchemaRegistry implements AvroSchemaRegistry {

    public static final String AVRO_SCHEMAS_PATH_PROPERTY = "hazelcast.avro.schemasPath";

    private static final String AVRO_SCHEMAS_PATH = System.getProperty(AVRO_SCHEMAS_PATH_PROPERTY,
            Paths.get(".", "avsc").toAbsolutePath().normalize().toString());
    private static final Path AVRO_SCHEMAS_DIRECTORY;

    static {
        Path schemasPath = Paths.get(AVRO_SCHEMAS_PATH);
        if (Files.notExists(schemasPath)) {
            throw new IllegalArgumentException("hazelcast.avro.schemasPath must be a path to an existing directory containing "
                    + "Apache Avro schemas. Directory " + AVRO_SCHEMAS_PATH + " does not exist or is not readable "
                    + "by this process.");
        }
        AVRO_SCHEMAS_DIRECTORY = schemasPath;
    }

    // schemas is initialized in PathSchemaRegistry constructor and only updated by
    // single threaded file watcher
    volatile ConcurrentMap<Integer, Schema> schemasById;
    volatile ConcurrentMap<String, Schema[]> schemasByTypeName;
    final ILogger logger;
    final WatchService watchService;
    final WatchKey watchKey;

    public PathSchemaRegistry() throws IOException {
        logger = Logger.getLogger(PathSchemaRegistry.class);
        watchService = AVRO_SCHEMAS_DIRECTORY.getFileSystem().newWatchService();
        watchKey = initialize();
        startWatching();
    }

    void dumpKnownSchemas() {
        logger.info("Avro serializer scanned " + AVRO_SCHEMAS_DIRECTORY.toString() + " and found known schemas: ");
        for (Map.Entry<Integer, Schema> entry : schemasById.entrySet()) {
            logger.info(entry.getKey() + " [ " + entry.getValue().getFullName() + " ]");
        }
    }

    private WatchKey initialize() throws IOException {
        scanSchemas();
        return AVRO_SCHEMAS_DIRECTORY.register(watchService, StandardWatchEventKinds.ENTRY_CREATE,
                StandardWatchEventKinds.ENTRY_DELETE, StandardWatchEventKinds.ENTRY_MODIFY);
    }

    @Override
    public void scanSchemas() {
        ConcurrentMap<Integer, Schema> schemas = new ConcurrentHashMap<>();
        ConcurrentMap<String, Schema[]> schemasByTypeName = new ConcurrentHashMap<>();
        try (Stream<Path> schemaFiles = Files.list(AVRO_SCHEMAS_DIRECTORY)) {
            schemaFiles.forEach(file -> {
                if (Files.isDirectory(file)) {
                    return;
                }
                Schema.Parser parser = new Schema.Parser();
                try {
                    Schema schema = parser.parse(file.toFile());
                    schemas.put(schema.hashCode(), schema);
                    String schemaName = schemaName(schema, file);
                    int version = versionOf(file);
                    if (schemasByTypeName.containsKey(schemaName)) {
                        Schema[] oldSchemas = schemasByTypeName.get(schemaName);
                        Schema[] newSchemas;
                        if (oldSchemas.length < version) {
                            newSchemas = Arrays.copyOf(oldSchemas, version);
                        } else {
                            newSchemas = oldSchemas;
                        }
                        newSchemas[version] = schema;
                        schemasByTypeName.put(schemaName, newSchemas);
                    } else {
                        schemasByTypeName.put(schemaName, new Schema[] {schema});
                    }
                } catch (Exception e) {
                    // log
                    logger.warning("File " + file.toString() + " could not be parsed, probably not an Avro schema?",
                            e);
                }
            });
        } catch (IOException e) {
            logger.warning("Could not list files in Avro schemas directory " + AVRO_SCHEMAS_DIRECTORY, e);
        }
        synchronized (this) {
            this.schemasById = schemas;
            this.schemasByTypeName = schemasByTypeName;
        }
        dumpKnownSchemas();
    }

    private void startWatching() {
        Thread t = new Thread(() -> {
            while (true) {
                try {
                    WatchKey watchKey = watchService.take();
                    // don't care about the actual events, just make sure they are consumed
                    watchKey.pollEvents();
                    scanSchemas();
                    boolean valid = watchKey.reset();
                    if (!valid) {
                        break;
                    }
                } catch (InterruptedException | ClosedWatchServiceException e) {
                    break;
                }
            }
        });
        t.start();
    }

    private String schemaName(Schema schema, Path file) {
        return schema.getFullName();
    }

    public Schema getSchema(int schemaId) {
        synchronized (this) {
            return schemasById.get(schemaId);
        }
    }

    @Override
    public Schema getSchema(String typeName) {
        synchronized (this) {
            Schema[] schemas = schemasByTypeName.get(typeName);
            return schemas[schemas.length - 1];
        }
    }

    public Schema getSchema(String typeName, int version) {
        synchronized (this) {
            return schemasByTypeName.get(typeName)[version];
        }
    }

    @Override
    public void destroy() {
        watchKey.cancel();
        try {
            watchService.close();
        } catch (IOException e) {
            // ignore
        }
    }

    private int versionOf(Path file) {
        String filename = file.getFileName().toString();
        // trim .
        filename = filename.substring(0, filename.lastIndexOf('.'));
        int lastUnderscore = filename.lastIndexOf('_');
        if (lastUnderscore > -1) {
            String version = filename.substring(lastUnderscore);
            try {
                return Integer.parseInt(version);
            } catch (NumberFormatException e) {
                logger.warning("Filename " + file.toAbsolutePath().normalize().toString() + " not properly formatted");
                return 0;
            }
        }
        return 0;
    }
}
