package com.hazelcast.bitset.impl.operations;

import com.hazelcast.bitset.BitSetDataSerializerHook;
import com.hazelcast.bitset.impl.BitSetContainer;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.util.BitSet;

public class AndOperation extends AbstractBitSetOperation implements BackupAwareOperation {

    private BitSet bitSet;

    public AndOperation() {
    }

    public AndOperation(String name, BitSet bitSet) {
        super(name);
        this.bitSet = bitSet;
    }

    public AndOperation(String name, BitSetContainer bitSetContainer) {
        super(name);
        this.bitSet = bitSetContainer.getBitSet();
    }

    @Override
    public int getId() {
        return BitSetDataSerializerHook.AND_OPERATION;
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

    @Override
    public boolean shouldBackup() {
        return true;
    }

    @Override
    public int getSyncBackupCount() {
        return 1;
    }

    @Override
    public int getAsyncBackupCount() {
        return 0;
    }

    @Override
    public Operation getBackupOperation() {
        return new AndBackupOperation(name, bitSet);
    }
}
