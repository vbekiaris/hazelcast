package com.hazelcast.internal.dynamicconfig;

import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.CardinalityEstimatorConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.ConfigPatternMatcher;
import com.hazelcast.config.DurableExecutorConfig;
import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.HotRestartPersistenceConfig;
import com.hazelcast.config.JobTrackerConfig;
import com.hazelcast.config.ListConfig;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.LockConfig;
import com.hazelcast.config.ManagementCenterConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MemberAttributeConfig;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.PartitionGroupConfig;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.config.QuorumConfig;
import com.hazelcast.config.ReliableTopicConfig;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.ScheduledExecutorConfig;
import com.hazelcast.config.SecurityConfig;
import com.hazelcast.config.SemaphoreConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.ServicesConfig;
import com.hazelcast.config.SetConfig;
import com.hazelcast.config.TopicConfig;
import com.hazelcast.config.UserCodeDeploymentConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ManagedContext;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.File;
import java.net.URL;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentMap;

public class DynamicConfig extends Config {
    private final Config delegate;

    private final ConfigurationService configurationService;

    public DynamicConfig(Config delegate, HazelcastInstance instance) {
        this.delegate = delegate;

        Node node = ((HazelcastInstanceImpl) instance).node;
        NodeEngineImpl nodeEngine = node.getNodeEngine();

        configurationService = nodeEngine.getConfigurationService();
    }

    @Override
    public ClassLoader getClassLoader() {
        return delegate.getClassLoader();
    }

    @Override
    public Config setClassLoader(ClassLoader classLoader) {
        throw new UnsupportedOperationException("Classloader cannot be changed in a running member.");
    }

    @Override
    public ConfigPatternMatcher getConfigPatternMatcher() {
        return delegate.getConfigPatternMatcher();
    }

    @Override
    public void setConfigPatternMatcher(ConfigPatternMatcher configPatternMatcher) {
        throw new UnsupportedOperationException("Config Pattern Matcher cannot be changed in a running member.");
    }

    @Override
    public String getProperty(String name) {
        return delegate.getProperty(name);
    }

    @Override
    public Config setProperty(String name, String value) {
        throw new UnsupportedOperationException("Properties cannot be changed in a running member.");
    }

    @Override
    public MemberAttributeConfig getMemberAttributeConfig() {
        return delegate.getMemberAttributeConfig().asReadOnly();
    }

    @Override
    public void setMemberAttributeConfig(MemberAttributeConfig memberAttributeConfig) {
        throw new UnsupportedOperationException("Member attributes cannot be changed in a running member.");
    }

    @Override
    public Properties getProperties() {
        return delegate.getProperties();
    }

    @Override
    public Config setProperties(Properties properties) {
        throw new UnsupportedOperationException("Properties cannot be changed in a running member.");
    }

    @Override
    public String getInstanceName() {
        return delegate.getInstanceName();
    }

    @Override
    public Config setInstanceName(String instanceName) {
        throw new UnsupportedOperationException("Instance name cannot be changed in a running member.");
    }

    @Override
    public GroupConfig getGroupConfig() {
        //todo: should we implement a read-only version of GroupConfig?
        return delegate.getGroupConfig();
    }

    @Override
    public Config setGroupConfig(GroupConfig groupConfig) {
        throw new UnsupportedOperationException("Group config cannot be changed in a running member.");
    }

    @Override
    public NetworkConfig getNetworkConfig() {
        //todo: should we implement a read-only version of NetworkConfig
        return delegate.getNetworkConfig();
    }

    @Override
    public Config setNetworkConfig(NetworkConfig networkConfig) {
        throw new UnsupportedOperationException("Network config cannot be changed in a running member.");
    }

    @Override
    public MapConfig findMapConfig(String name) {
        return delegate.findMapConfig(name);
    }

    @Override
    public MapConfig getMapConfig(String name) {
        return delegate.getMapConfig(name);
    }

    @Override
    public Config addMapConfig(MapConfig mapConfig) {
        configurationService.broadcastConfig(mapConfig);
        return this;
    }

    @Override
    public Map<String, MapConfig> getMapConfigs() {
        return delegate.getMapConfigs();
    }

    @Override
    public Config setMapConfigs(Map<String, MapConfig> mapConfigs) {
        return delegate.setMapConfigs(mapConfigs);
    }

    @Override
    public CacheSimpleConfig findCacheConfig(String name) {
        return delegate.findCacheConfig(name);
    }

    @Override
    public CacheSimpleConfig getCacheConfig(String name) {
        return delegate.getCacheConfig(name);
    }

    @Override
    public Config addCacheConfig(CacheSimpleConfig cacheConfig) {
        return delegate.addCacheConfig(cacheConfig);
    }

    @Override
    public Map<String, CacheSimpleConfig> getCacheConfigs() {
        return delegate.getCacheConfigs();
    }

    @Override
    public Config setCacheConfigs(Map<String, CacheSimpleConfig> cacheConfigs) {
        return delegate.setCacheConfigs(cacheConfigs);
    }

    @Override
    public QueueConfig findQueueConfig(String name) {
        return delegate.findQueueConfig(name);
    }

    @Override
    public QueueConfig getQueueConfig(String name) {
        return delegate.getQueueConfig(name);
    }

    @Override
    public Config addQueueConfig(QueueConfig queueConfig) {
        return delegate.addQueueConfig(queueConfig);
    }

    @Override
    public Map<String, QueueConfig> getQueueConfigs() {
        return delegate.getQueueConfigs();
    }

    @Override
    public Config setQueueConfigs(Map<String, QueueConfig> queueConfigs) {
        return delegate.setQueueConfigs(queueConfigs);
    }

    @Override
    public LockConfig findLockConfig(String name) {
        return delegate.findLockConfig(name);
    }

    @Override
    public LockConfig getLockConfig(String name) {
        return delegate.getLockConfig(name);
    }

    @Override
    public Config addLockConfig(LockConfig lockConfig) {
        return delegate.addLockConfig(lockConfig);
    }

    @Override
    public Map<String, LockConfig> getLockConfigs() {
        return delegate.getLockConfigs();
    }

    @Override
    public Config setLockConfigs(Map<String, LockConfig> lockConfigs) {
        return delegate.setLockConfigs(lockConfigs);
    }

    @Override
    public ListConfig findListConfig(String name) {
        return delegate.findListConfig(name);
    }

    @Override
    public ListConfig getListConfig(String name) {
        return delegate.getListConfig(name);
    }

    @Override
    public Config addListConfig(ListConfig listConfig) {
        return delegate.addListConfig(listConfig);
    }

    @Override
    public Map<String, ListConfig> getListConfigs() {
        return delegate.getListConfigs();
    }

    @Override
    public Config setListConfigs(Map<String, ListConfig> listConfigs) {
        return delegate.setListConfigs(listConfigs);
    }

    @Override
    public SetConfig findSetConfig(String name) {
        return delegate.findSetConfig(name);
    }

    @Override
    public SetConfig getSetConfig(String name) {
        return delegate.getSetConfig(name);
    }

    @Override
    public Config addSetConfig(SetConfig setConfig) {
        return delegate.addSetConfig(setConfig);
    }

    @Override
    public Map<String, SetConfig> getSetConfigs() {
        return delegate.getSetConfigs();
    }

    @Override
    public Config setSetConfigs(Map<String, SetConfig> setConfigs) {
        return delegate.setSetConfigs(setConfigs);
    }

    @Override
    public MultiMapConfig findMultiMapConfig(String name) {
        return delegate.findMultiMapConfig(name);
    }

    @Override
    public MultiMapConfig getMultiMapConfig(String name) {
        return delegate.getMultiMapConfig(name);
    }

    @Override
    public Config addMultiMapConfig(MultiMapConfig multiMapConfig) {
        configurationService.broadcastConfig(multiMapConfig);
        return this;
    }

    @Override
    public Map<String, MultiMapConfig> getMultiMapConfigs() {
        return delegate.getMultiMapConfigs();
    }

    @Override
    public Config setMultiMapConfigs(Map<String, MultiMapConfig> multiMapConfigs) {
        return delegate.setMultiMapConfigs(multiMapConfigs);
    }

    @Override
    public ReplicatedMapConfig findReplicatedMapConfig(String name) {
        return delegate.findReplicatedMapConfig(name);
    }

    @Override
    public ReplicatedMapConfig getReplicatedMapConfig(String name) {
        return delegate.getReplicatedMapConfig(name);
    }

    @Override
    public Config addReplicatedMapConfig(ReplicatedMapConfig replicatedMapConfig) {
        return delegate.addReplicatedMapConfig(replicatedMapConfig);
    }

    @Override
    public Map<String, ReplicatedMapConfig> getReplicatedMapConfigs() {
        return delegate.getReplicatedMapConfigs();
    }

    @Override
    public Config setReplicatedMapConfigs(Map<String, ReplicatedMapConfig> replicatedMapConfigs) {
        return delegate.setReplicatedMapConfigs(replicatedMapConfigs);
    }

    @Override
    public RingbufferConfig findRingbufferConfig(String name) {
        return delegate.findRingbufferConfig(name);
    }

    @Override
    public RingbufferConfig getRingbufferConfig(String name) {
        return delegate.getRingbufferConfig(name);
    }

    @Override
    public Config addRingBufferConfig(RingbufferConfig ringbufferConfig) {
        return delegate.addRingBufferConfig(ringbufferConfig);
    }

    @Override
    public Map<String, RingbufferConfig> getRingbufferConfigs() {
        return delegate.getRingbufferConfigs();
    }

    @Override
    public Config setRingbufferConfigs(Map<String, RingbufferConfig> ringbufferConfigs) {
        return delegate.setRingbufferConfigs(ringbufferConfigs);
    }

    @Override
    public TopicConfig findTopicConfig(String name) {
        return delegate.findTopicConfig(name);
    }

    @Override
    public TopicConfig getTopicConfig(String name) {
        return delegate.getTopicConfig(name);
    }

    @Override
    public Config addTopicConfig(TopicConfig topicConfig) {
        return delegate.addTopicConfig(topicConfig);
    }

    @Override
    public ReliableTopicConfig findReliableTopicConfig(String name) {
        return delegate.findReliableTopicConfig(name);
    }

    @Override
    public ReliableTopicConfig getReliableTopicConfig(String name) {
        return delegate.getReliableTopicConfig(name);
    }

    @Override
    public Map<String, ReliableTopicConfig> getReliableTopicConfigs() {
        return delegate.getReliableTopicConfigs();
    }

    @Override
    public Config addReliableTopicConfig(ReliableTopicConfig topicConfig) {
        return delegate.addReliableTopicConfig(topicConfig);
    }

    @Override
    public Config setReliableTopicConfigs(Map<String, ReliableTopicConfig> reliableTopicConfigs) {
        return delegate.setReliableTopicConfigs(reliableTopicConfigs);
    }

    @Override
    public Map<String, TopicConfig> getTopicConfigs() {
        return delegate.getTopicConfigs();
    }

    @Override
    public Config setTopicConfigs(Map<String, TopicConfig> mapTopicConfigs) {
        return delegate.setTopicConfigs(mapTopicConfigs);
    }

    @Override
    public ExecutorConfig findExecutorConfig(String name) {
        return delegate.findExecutorConfig(name);
    }

    @Override
    public DurableExecutorConfig findDurableExecutorConfig(String name) {
        return delegate.findDurableExecutorConfig(name);
    }

    @Override
    public ScheduledExecutorConfig findScheduledExecutorConfig(String name) {
        return delegate.findScheduledExecutorConfig(name);
    }

    @Override
    public CardinalityEstimatorConfig findCardinalityEstimatorConfig(String name) {
        return delegate.findCardinalityEstimatorConfig(name);
    }

    @Override
    public ExecutorConfig getExecutorConfig(String name) {
        return delegate.getExecutorConfig(name);
    }

    @Override
    public DurableExecutorConfig getDurableExecutorConfig(String name) {
        return delegate.getDurableExecutorConfig(name);
    }

    @Override
    public ScheduledExecutorConfig getScheduledExecutorConfig(String name) {
        return delegate.getScheduledExecutorConfig(name);
    }

    @Override
    public CardinalityEstimatorConfig getCardinalityEstimatorConfig(String name) {
        return delegate.getCardinalityEstimatorConfig(name);
    }

    @Override
    public Config addExecutorConfig(ExecutorConfig executorConfig) {
        return delegate.addExecutorConfig(executorConfig);
    }

    @Override
    public Config addDurableExecutorConfig(DurableExecutorConfig durableExecutorConfig) {
        return delegate.addDurableExecutorConfig(durableExecutorConfig);
    }

    @Override
    public Config addScheduledExecutorConfig(ScheduledExecutorConfig scheduledExecutorConfig) {
        return delegate.addScheduledExecutorConfig(scheduledExecutorConfig);
    }

    @Override
    public Config addCardinalityEstimatorConfig(CardinalityEstimatorConfig cardinalityEstimatorConfig) {
        return delegate.addCardinalityEstimatorConfig(cardinalityEstimatorConfig);
    }

    @Override
    public Map<String, ExecutorConfig> getExecutorConfigs() {
        return delegate.getExecutorConfigs();
    }

    @Override
    public Config setExecutorConfigs(Map<String, ExecutorConfig> executorConfigs) {
        return delegate.setExecutorConfigs(executorConfigs);
    }

    @Override
    public Map<String, DurableExecutorConfig> getDurableExecutorConfigs() {
        return delegate.getDurableExecutorConfigs();
    }

    @Override
    public Config setDurableExecutorConfigs(Map<String, DurableExecutorConfig> durableExecutorConfigs) {
        return delegate.setDurableExecutorConfigs(durableExecutorConfigs);
    }

    @Override
    public Map<String, ScheduledExecutorConfig> getScheduledExecutorConfigs() {
        return delegate.getScheduledExecutorConfigs();
    }

    @Override
    public Config setScheduledExecutorConfigs(Map<String, ScheduledExecutorConfig> scheduledExecutorConfigs) {
        return delegate.setScheduledExecutorConfigs(scheduledExecutorConfigs);
    }

    @Override
    public Map<String, CardinalityEstimatorConfig> getCardinalityEstimatorConfigs() {
        return delegate.getCardinalityEstimatorConfigs();
    }

    @Override
    public Config setCardinalityEstimatorConfigs(Map<String, CardinalityEstimatorConfig> cardinalityEstimatorConfigs) {
        return delegate.setCardinalityEstimatorConfigs(cardinalityEstimatorConfigs);
    }

    @Override
    public SemaphoreConfig findSemaphoreConfig(String name) {
        return delegate.findSemaphoreConfig(name);
    }

    @Override
    public SemaphoreConfig getSemaphoreConfig(String name) {
        return delegate.getSemaphoreConfig(name);
    }

    @Override
    public Config addSemaphoreConfig(SemaphoreConfig semaphoreConfig) {
        return delegate.addSemaphoreConfig(semaphoreConfig);
    }

    @Override
    public Collection<SemaphoreConfig> getSemaphoreConfigs() {
        return delegate.getSemaphoreConfigs();
    }

    @Override
    public Config setSemaphoreConfigs(Map<String, SemaphoreConfig> semaphoreConfigs) {
        return delegate.setSemaphoreConfigs(semaphoreConfigs);
    }

    @Override
    public WanReplicationConfig getWanReplicationConfig(String name) {
        return delegate.getWanReplicationConfig(name);
    }

    @Override
    public Config addWanReplicationConfig(WanReplicationConfig wanReplicationConfig) {
        return delegate.addWanReplicationConfig(wanReplicationConfig);
    }

    @Override
    public Map<String, WanReplicationConfig> getWanReplicationConfigs() {
        return delegate.getWanReplicationConfigs();
    }

    @Override
    public Config setWanReplicationConfigs(Map<String, WanReplicationConfig> wanReplicationConfigs) {
        return delegate.setWanReplicationConfigs(wanReplicationConfigs);
    }

    @Override
    public JobTrackerConfig findJobTrackerConfig(String name) {
        return delegate.findJobTrackerConfig(name);
    }

    @Override
    public JobTrackerConfig getJobTrackerConfig(String name) {
        return delegate.getJobTrackerConfig(name);
    }

    @Override
    public Config addJobTrackerConfig(JobTrackerConfig jobTrackerConfig) {
        return delegate.addJobTrackerConfig(jobTrackerConfig);
    }

    @Override
    public Map<String, JobTrackerConfig> getJobTrackerConfigs() {
        return delegate.getJobTrackerConfigs();
    }

    @Override
    public Config setJobTrackerConfigs(Map<String, JobTrackerConfig> jobTrackerConfigs) {
        return delegate.setJobTrackerConfigs(jobTrackerConfigs);
    }

    @Override
    public Map<String, QuorumConfig> getQuorumConfigs() {
        return delegate.getQuorumConfigs();
    }

    @Override
    public QuorumConfig getQuorumConfig(String name) {
        return delegate.getQuorumConfig(name);
    }

    @Override
    public QuorumConfig findQuorumConfig(String name) {
        return delegate.findQuorumConfig(name);
    }

    @Override
    public Config setQuorumConfigs(Map<String, QuorumConfig> quorumConfigs) {
        return delegate.setQuorumConfigs(quorumConfigs);
    }

    @Override
    public Config addQuorumConfig(QuorumConfig quorumConfig) {
        return delegate.addQuorumConfig(quorumConfig);
    }

    @Override
    public ManagementCenterConfig getManagementCenterConfig() {
        return delegate.getManagementCenterConfig();
    }

    @Override
    public Config setManagementCenterConfig(ManagementCenterConfig managementCenterConfig) {
        return delegate.setManagementCenterConfig(managementCenterConfig);
    }

    @Override
    public ServicesConfig getServicesConfig() {
        return delegate.getServicesConfig();
    }

    @Override
    public Config setServicesConfig(ServicesConfig servicesConfig) {
        return delegate.setServicesConfig(servicesConfig);
    }

    @Override
    public SecurityConfig getSecurityConfig() {
        return delegate.getSecurityConfig();
    }

    @Override
    public Config setSecurityConfig(SecurityConfig securityConfig) {
        return delegate.setSecurityConfig(securityConfig);
    }

    @Override
    public Config addListenerConfig(ListenerConfig listenerConfig) {
        return delegate.addListenerConfig(listenerConfig);
    }

    @Override
    public List<ListenerConfig> getListenerConfigs() {
        return delegate.getListenerConfigs();
    }

    @Override
    public Config setListenerConfigs(List<ListenerConfig> listenerConfigs) {
        return delegate.setListenerConfigs(listenerConfigs);
    }

    @Override
    public SerializationConfig getSerializationConfig() {
        return delegate.getSerializationConfig();
    }

    @Override
    public Config setSerializationConfig(SerializationConfig serializationConfig) {
        return delegate.setSerializationConfig(serializationConfig);
    }

    @Override
    public PartitionGroupConfig getPartitionGroupConfig() {
        return delegate.getPartitionGroupConfig();
    }

    @Override
    public Config setPartitionGroupConfig(PartitionGroupConfig partitionGroupConfig) {
        return delegate.setPartitionGroupConfig(partitionGroupConfig);
    }

    @Override
    public HotRestartPersistenceConfig getHotRestartPersistenceConfig() {
        return delegate.getHotRestartPersistenceConfig();
    }

    @Override
    public Config setHotRestartPersistenceConfig(HotRestartPersistenceConfig hrConfig) {
        return delegate.setHotRestartPersistenceConfig(hrConfig);
    }

    @Override
    public ManagedContext getManagedContext() {
        return delegate.getManagedContext();
    }

    @Override
    public Config setManagedContext(ManagedContext managedContext) {
        return delegate.setManagedContext(managedContext);
    }

    @Override
    public ConcurrentMap<String, Object> getUserContext() {
        return delegate.getUserContext();
    }

    @Override
    public Config setUserContext(ConcurrentMap<String, Object> userContext) {
        return delegate.setUserContext(userContext);
    }

    @Override
    public NativeMemoryConfig getNativeMemoryConfig() {
        return delegate.getNativeMemoryConfig();
    }

    @Override
    public Config setNativeMemoryConfig(NativeMemoryConfig nativeMemoryConfig) {
        return delegate.setNativeMemoryConfig(nativeMemoryConfig);
    }

    @Override
    public URL getConfigurationUrl() {
        return delegate.getConfigurationUrl();
    }

    @Override
    public Config setConfigurationUrl(URL configurationUrl) {
        return delegate.setConfigurationUrl(configurationUrl);
    }

    @Override
    public File getConfigurationFile() {
        return delegate.getConfigurationFile();
    }

    @Override
    public Config setConfigurationFile(File configurationFile) {
        return delegate.setConfigurationFile(configurationFile);
    }

    @Override
    public String getLicenseKey() {
        return delegate.getLicenseKey();
    }

    @Override
    public Config setLicenseKey(String licenseKey) {
        return delegate.setLicenseKey(licenseKey);
    }

    @Override
    public boolean isLiteMember() {
        return delegate.isLiteMember();
    }

    @Override
    public Config setLiteMember(boolean liteMember) {
        return delegate.setLiteMember(liteMember);
    }

    @Override
    public UserCodeDeploymentConfig getUserCodeDeploymentConfig() {
        return delegate.getUserCodeDeploymentConfig();
    }

    @Override
    public Config setUserCodeDeploymentConfig(UserCodeDeploymentConfig userCodeDeploymentConfig) {
        return delegate.setUserCodeDeploymentConfig(userCodeDeploymentConfig);
    }

    @Override
    public String toString() {
        return delegate.toString();
    }

}
