package com.hazelcast.bitset.impl.operations;

import com.hazelcast.bitset.BitSetDataSerializerHook;
import com.hazelcast.bitset.impl.BitSetContainer;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.BackupOperation;

import java.io.IOException;

public class OrBackupOperation
        extends AbstractBitSetOperation implements BackupOperation {

    private BitSetContainer set;

    public OrBackupOperation() {
    }

    public OrBackupOperation(String name, BitSetContainer set) {
        super(name);
        this.set = set;
    }

    @Override
    public void run()
            throws Exception {
        getContainer().or(set);
    }

    @Override
    public int getId() {
        return BitSetDataSerializerHook.OR_BACKUP_OPERATION;
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        this.set = in.readObject();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        out.writeObject(set);
    }
}
