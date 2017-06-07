package com.hazelcast.internal.dynamicconfig;

import com.hazelcast.config.CardinalityEstimatorConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.DurableExecutorConfig;
import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.config.ListConfig;
import com.hazelcast.config.LockConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.ScheduledExecutorConfig;
import com.hazelcast.config.SemaphoreConfig;
import com.hazelcast.config.SetConfig;
import com.hazelcast.config.TopicConfig;
import com.hazelcast.internal.cluster.ClusterVersionListener;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.CoreService;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.PostJoinAwareService;
import com.hazelcast.version.Version;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.internal.cluster.Versions.V3_8;
import static com.hazelcast.util.InvocationUtil.invokeOnStableCluster;

public class ConfigurationService implements PostJoinAwareService, MigrationAwareService,
        CoreService, ClusterVersionListener, ManagedService {
    public static final String SERVICE_NAME = "configuration-service";
    public static final int CONFIG_PUBLISH_MAX_ATTEMPT_COUNT = 100;

    private NodeEngine nodeEngine;
    private ConcurrentMap<String, MultiMapConfig> multiMapConfigs = new ConcurrentHashMap<String, MultiMapConfig>();
    private ConcurrentMap<String, MapConfig> mapConfigs = new ConcurrentHashMap<String, MapConfig>();
    private ConcurrentMap<String, CardinalityEstimatorConfig> cardinalityEstimatorConfigs =
            new ConcurrentHashMap<String, CardinalityEstimatorConfig>();
    private ConcurrentMap<String, RingbufferConfig> ringbufferConfigs = new ConcurrentHashMap<String, RingbufferConfig>();
    private ConcurrentMap<String, LockConfig> lockConfigs = new ConcurrentHashMap<String, LockConfig>();
    private ConcurrentMap<String, ListConfig> listConfigs = new ConcurrentHashMap<String, ListConfig>();
    private ConcurrentMap<String, SetConfig> setConfigs = new ConcurrentHashMap<String, SetConfig>();
    private ConcurrentMap<String, ReplicatedMapConfig> replicatedMapConfigs =
            new ConcurrentHashMap<String, ReplicatedMapConfig>();
    private ConcurrentMap<String, TopicConfig> topicConfigs = new ConcurrentHashMap<String, TopicConfig>();
    private ConcurrentMap<String, ExecutorConfig> executorConfigs = new ConcurrentHashMap<String, ExecutorConfig>();
    private ConcurrentMap<String, DurableExecutorConfig> durableExecutorConfigs =
            new ConcurrentHashMap<String, DurableExecutorConfig>();
    private ConcurrentMap<String, ScheduledExecutorConfig> scheduledExecutorConfigs =
            new ConcurrentHashMap<String, ScheduledExecutorConfig>();
    private ConcurrentMap<String, SemaphoreConfig> semaphoreConfigs = new ConcurrentHashMap<String, SemaphoreConfig>();

    private Config staticConfig;
    private volatile Version version;

    public ConfigurationService(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.staticConfig = nodeEngine.getConfig();
    }

    @Override
    public Operation getPostJoinOperation() {
        return null;
    }

    public MultiMapConfig getMultiMapConfig(String name) {
        Map<String, MultiMapConfig> staticMultiMapConfigs = staticConfig.getMultiMapConfigs();
        MultiMapConfig multiMapConfig = Config.lookupByPattern(staticMultiMapConfigs, name);
        if (multiMapConfig == null) {
            multiMapConfig = multiMapConfigs.get(name);
        }
        if (multiMapConfig == null) {
            multiMapConfig = staticConfig.getMultiMapConfig(name);
        }
        return multiMapConfig;
    }

    public MapConfig getMapConfig(String name) {
        Map<String, MapConfig> staticMapConfigs = staticConfig.getMapConfigs();
        MapConfig mapConfig = Config.lookupByPattern(staticMapConfigs, name);
        if (mapConfig == null) {
            mapConfig = mapConfigs.get(name);
        }
        if (mapConfig == null) {
            mapConfig = staticConfig.getMapConfig(name);
        }
        return mapConfig;
    }

    public CardinalityEstimatorConfig getCardinalityEstimatorConfig(String name) {
        Map<String, CardinalityEstimatorConfig> staticConfigs = staticConfig.getCardinalityEstimatorConfigs();
        CardinalityEstimatorConfig config = Config.lookupByPattern(staticConfigs, name);
        if (config == null) {
            config = cardinalityEstimatorConfigs.get(name);
        }
        if (config == null) {
            config = staticConfig.getCardinalityEstimatorConfig(name);
        }
        return config;
    }

    public ExecutorConfig getExecutorConfig(String name) {
        Map<String, ExecutorConfig> staticConfigs = staticConfig.getExecutorConfigs();
        ExecutorConfig config = Config.lookupByPattern(staticConfigs, name);
        if (config == null) {
            config = executorConfigs.get(name);
        }
        if (config == null) {
            config = staticConfig.getExecutorConfig(name);
        }
        return config;
    }

    public ScheduledExecutorConfig getScheduledExecutorConfig(String name) {
        Map<String, ScheduledExecutorConfig> staticConfigs = staticConfig.getScheduledExecutorConfigs();
        ScheduledExecutorConfig config = Config.lookupByPattern(staticConfigs, name);
        if (config == null) {
            config = scheduledExecutorConfigs.get(name);
        }
        if (config == null) {
            config = staticConfig.getScheduledExecutorConfig(name);
        }
        return config;
    }

    public DurableExecutorConfig getDurableExecutorConfig(String name) {
        Map<String, DurableExecutorConfig> staticConfigs = staticConfig.getDurableExecutorConfigs();
        DurableExecutorConfig config = Config.lookupByPattern(staticConfigs, name);
        if (config == null) {
            config = durableExecutorConfigs.get(name);
        }
        if (config == null) {
            config = staticConfig.getDurableExecutorConfig(name);
        }
        return config;
    }

    public SemaphoreConfig getSemaphoreConfig(String name) {
        Map<String, SemaphoreConfig> staticConfigs = staticConfig.getSemaphoreConfigsAsMap();
        SemaphoreConfig config = Config.lookupByPattern(staticConfigs, name);
        if (config == null) {
            config = semaphoreConfigs.get(name);
        }
        if (config == null) {
            config = staticConfig.getSemaphoreConfig(name);
        }
        return config;
    }

    public RingbufferConfig getRingbufferConfig(String name) {
        Map<String, RingbufferConfig> staticConfigs = staticConfig.getRingbufferConfigs();
        RingbufferConfig config = Config.lookupByPattern(staticConfigs, name);
        if (config == null) {
            config = ringbufferConfigs.get(name);
        }
        if (config == null) {
            config = staticConfig.getRingbufferConfig(name);
        }
        return config;
    }

    public LockConfig getLockConfig(String name) {
        Map<String, LockConfig> staticConfigs = staticConfig.getLockConfigs();
        LockConfig config = Config.lookupByPattern(staticConfigs, name);
        if (config == null) {
            config = lockConfigs.get(name);
        }
        if (config == null) {
            config = staticConfig.getLockConfig(name);
        }
        return config;
    }

    public ListConfig getListConfig(String name) {
        Map<String, ListConfig> staticConfigs = staticConfig.getListConfigs();
        ListConfig config = Config.lookupByPattern(staticConfigs, name);
        if (config == null) {
            config = listConfigs.get(name);
        }
        if (config == null) {
            config = staticConfig.getListConfig(name);
        }
        return config;
    }

    public void registerLocally(IdentifiedDataSerializable config) {
        if (config instanceof MultiMapConfig) {
            MultiMapConfig multiMapConfig = (MultiMapConfig) config;
            multiMapConfigs.putIfAbsent(multiMapConfig.getName(), multiMapConfig);
        } else if (config instanceof MapConfig) {
            MapConfig mapConfig = (MapConfig) config;
            mapConfigs.putIfAbsent(mapConfig.getName(), mapConfig);
        } else if (config instanceof CardinalityEstimatorConfig) {
            CardinalityEstimatorConfig cardinalityEstimatorConfig = (CardinalityEstimatorConfig) config;
            cardinalityEstimatorConfigs.putIfAbsent(cardinalityEstimatorConfig.getName(), cardinalityEstimatorConfig);
        } else if (config instanceof RingbufferConfig) {
            RingbufferConfig ringbufferConfig = (RingbufferConfig) config;
            ringbufferConfigs.putIfAbsent(ringbufferConfig.getName(), ringbufferConfig);
        } else if (config instanceof LockConfig) {
            LockConfig lockConfig = (LockConfig) config;
            lockConfigs.putIfAbsent(lockConfig.getName(), lockConfig);
        } else if (config instanceof ListConfig) {
            ListConfig listConfig = (ListConfig) config;
            listConfigs.putIfAbsent(listConfig.getName(), listConfig);
        } else if (config instanceof SetConfig) {
            SetConfig setConfig = (SetConfig) config;
            setConfigs.putIfAbsent(setConfig.getName(), setConfig);
        } else if (config instanceof ReplicatedMapConfig) {
            ReplicatedMapConfig replicatedMapConfig = (ReplicatedMapConfig) config;
            replicatedMapConfigs.putIfAbsent(replicatedMapConfig.getName(), replicatedMapConfig);
        } else if (config instanceof TopicConfig) {
            TopicConfig topicConfig = (TopicConfig) config;
            topicConfigs.putIfAbsent(topicConfig.getName(), topicConfig);
        } else if (config instanceof ExecutorConfig) {
            ExecutorConfig executorConfig = (ExecutorConfig) config;
            executorConfigs.putIfAbsent(executorConfig.getName(), executorConfig);
        } else if (config instanceof DurableExecutorConfig) {
            DurableExecutorConfig durableExecutorConfig = (DurableExecutorConfig) config;
            durableExecutorConfigs.putIfAbsent(durableExecutorConfig.getName(), durableExecutorConfig);
        } else if (config instanceof ScheduledExecutorConfig) {
            ScheduledExecutorConfig scheduledExecutorConfig = (ScheduledExecutorConfig) config;
            scheduledExecutorConfigs.putIfAbsent(scheduledExecutorConfig.getName(), scheduledExecutorConfig);
        } else if (config instanceof SemaphoreConfig) {
            SemaphoreConfig semaphoreConfig = (SemaphoreConfig) config;
            semaphoreConfigs.putIfAbsent(semaphoreConfig.getName(), semaphoreConfig);
        } else {
            throw new UnsupportedOperationException("Unsupported config type: " + config);
        }
    }

    public void broadcastConfig(IdentifiedDataSerializable config) {
        invokeOnStableCluster(nodeEngine, new AddDynamicConfigOperationFactory(config), null,
                CONFIG_PUBLISH_MAX_ATTEMPT_COUNT);
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        if (version.isLessOrEqual(V3_8)) {
            return null;
        }
        return new DynamicConfigReplicationOperation(multiMapConfigs, mapConfigs);
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent event) {
        //no-op
    }

    @Override
    public void commitMigration(PartitionMigrationEvent event) {
        //no-op
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {
        //no-op
    }

    @Override
    public void onClusterVersionChange(Version newVersion) {
        version = newVersion;
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        //no-op
    }

    @Override
    public void reset() {
        multiMapConfigs.clear();
    }

    @Override
    public void shutdown(boolean terminate) {
        //no-op
    }
}
