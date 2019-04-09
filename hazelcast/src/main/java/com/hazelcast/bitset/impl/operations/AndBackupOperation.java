package com.hazelcast.bitset.impl.operations;

import com.hazelcast.bitset.BitSetDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.BitSet;

public class AndBackupOperation
        extends AbstractBitSetOperation {

    private BitSet bitSet;

    public AndBackupOperation() {
    }

    public AndBackupOperation(String name, BitSet bitSet) {
        super(name);
        this.bitSet = bitSet;
    }

    @Override
    public int getId() {
        return BitSetDataSerializerHook.AND_BACKUP_OPERATION;
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        bitSet = in.readObject();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        out.writeObject(bitSet);
    }

    @Override
    public void run()
            throws Exception {
        if (bitSet.isEmpty()) {
            getContainer().clear();
        } else {
            getContainer().getBitSet().and(bitSet);
        }
    }
}
