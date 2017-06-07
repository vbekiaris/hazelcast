package com.hazelcast.internal.dynamicconfig;

import com.hazelcast.config.ConfigDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

public class AddDynamicConfigOperation extends AbstractDynamicConfigOperation {

    private IdentifiedDataSerializable config;

    public AddDynamicConfigOperation() {

    }

    public AddDynamicConfigOperation(IdentifiedDataSerializable config) {
        this.config = config;
    }

    @Override
    public void run() throws Exception {
        ConfigurationService service = getService();
        service.registerLocally(config);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeObject(config);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        config = in.readObject();
    }

    @Override
    public int getId() {
        return ConfigDataSerializerHook.ADD_DYNAMIC_CONFIG_OP;
    }

}
