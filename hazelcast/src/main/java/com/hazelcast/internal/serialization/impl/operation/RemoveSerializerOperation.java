package com.hazelcast.internal.serialization.impl.operation;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.SerializationDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;

import static com.hazelcast.internal.serialization.impl.SerializationDataSerializerHook.REMOVE_SERIALIZER_OPERATION;

public class RemoveSerializerOperation extends Operation implements IdentifiedDataSerializable {
    private int contextId;
    private String typeName;

    public RemoveSerializerOperation() {
    }

    public RemoveSerializerOperation(int contextId, String typeName) {
        this.contextId = contextId;
        this.typeName = typeName;
    }

    @Override
    public void run()
            throws Exception {
        InternalSerializationService serializationService =
                (InternalSerializationService) getNodeEngine().getSerializationService();
        serializationService.unregisterSerializer(contextId, typeName);
    }

    @Override
    public int getFactoryId() {
        return SerializationDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return REMOVE_SERIALIZER_OPERATION;
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        contextId = in.readInt();
        typeName = in.readUTF();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        out.writeInt(contextId);
        out.writeUTF(typeName);
    }
}
