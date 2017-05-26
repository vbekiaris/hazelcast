package com.hazelcast.internal.dynamicconfig;

import com.hazelcast.config.ConfigDataSerializerHook;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class DynamicConfigReplicationOperation extends Operation implements IdentifiedDataSerializable {

    private IdentifiedDataSerializable[] configs;

    public DynamicConfigReplicationOperation(Map<?, ? extends IdentifiedDataSerializable>...configs) {
        List<IdentifiedDataSerializable> allConfigs = new ArrayList<IdentifiedDataSerializable>();
        for (Map<?, ? extends IdentifiedDataSerializable> config : configs) {
            allConfigs.addAll(config.values());
        }
        this.configs = allConfigs.toArray(new IdentifiedDataSerializable[0]);
    }

    public DynamicConfigReplicationOperation() {

    }

    @Override
    public void run() throws Exception {
        ConfigurationService service = getService();
        for (IdentifiedDataSerializable multiMapConfig : configs) {
            service.registerLocally(multiMapConfig);
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeInt(configs.length);
        for (IdentifiedDataSerializable config: configs) {
            out.writeObject(config);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        configs = new IdentifiedDataSerializable[size];
        for (int i = 0; i < size; i++) {
            configs[i] = in.readObject();
        }
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return ConfigDataSerializerHook.REPLICATE_CONFIGURATIONS_OP;
    }
}
