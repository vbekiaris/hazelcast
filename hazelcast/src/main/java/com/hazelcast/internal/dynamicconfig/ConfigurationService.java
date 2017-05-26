package com.hazelcast.internal.dynamicconfig;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.Member;
import com.hazelcast.core.Partition;
import com.hazelcast.core.PartitionService;
import com.hazelcast.internal.cluster.ClusterVersionListener;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.CoreService;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.PostJoinAwareService;
import com.hazelcast.util.FutureUtil;
import com.hazelcast.version.Version;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.cluster.Versions.V3_8;
import static com.hazelcast.util.FutureUtil.RETHROW_ALL_EXCEPT_MEMBER_LEFT;

public class ConfigurationService implements PostJoinAwareService, MigrationAwareService,
        CoreService, ClusterVersionListener, ManagedService {
    public static final String SERVICE_NAME = "configuration-service";
    private static final int CONFIG_PUBLISH_MAX_ATTEMPT_COUNT = 100;

    private NodeEngine nodeEngine;
    private ConcurrentMap<String, MultiMapConfig> multiMapConfigs = new ConcurrentHashMap<String, MultiMapConfig>();
    private ConcurrentMap<String, MapConfig> mapConfigs = new ConcurrentHashMap<String, MapConfig>();
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

    public void registerLocally(IdentifiedDataSerializable config) {
        if (config instanceof MultiMapConfig) {
            MultiMapConfig multiMapConfig = (MultiMapConfig) config;
            multiMapConfigs.putIfAbsent(multiMapConfig.getName(), multiMapConfig);
        } else if (config instanceof MapConfig) {
            MapConfig mapConfig = (MapConfig) config;
            mapConfigs.putIfAbsent(mapConfig.getName(), mapConfig);
        } else {
            throw new UnsupportedOperationException("Unsupported config type: " + config);
        }
    }

    public void broadcastConfig(IdentifiedDataSerializable config) {
        warmUpPartitions();
        Collection<Member> originalMembers;
        int iterationCounter = 0;
        do {
            originalMembers = nodeEngine.getClusterService().getMembers();
            OperationService operationService = nodeEngine.getOperationService();
            List<Future> futures = new ArrayList<Future>(originalMembers.size());
            for (Member member : originalMembers) {
                Operation op = new AddDynamicConfigOperation(config);
                Address address = member.getAddress();
                InternalCompletableFuture<Object> future = operationService.invokeOnTarget(SERVICE_NAME, op, address);
                futures.add(future);
            }
            FutureUtil.waitWithDeadline(futures, 1, TimeUnit.MINUTES, RETHROW_ALL_EXCEPT_MEMBER_LEFT);
            Collection<Member> currentMembers = nodeEngine.getClusterService().getMembers();
            if (currentMembers.equals(originalMembers)) {
                break;
            }
            if (iterationCounter++ == CONFIG_PUBLISH_MAX_ATTEMPT_COUNT) {
                //todo better error message
                throw new HazelcastException("Cluster topology keeps changing, cannot distribute configuration " + config);
            }
        } while (!originalMembers.equals(nodeEngine.getClusterService().getMembers()));
    }

    //todo: move elsewhere
    private void warmUpPartitions() {
        final PartitionService ps = nodeEngine.getHazelcastInstance().getPartitionService();
        for (Partition partition : ps.getPartitions()) {
            while (partition.getOwner() == null) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new HazelcastException("Thread interrupted while initializing a partition table", e);
                }
            }
        }
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
