package domain;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;

import java.io.IOException;

public class SessionSerializer implements StreamSerializer {

    @Override
    public void write(ObjectDataOutput out, Object object)
            throws IOException {
        if (object instanceof Session) {
            out.writeInt(((Session) object).getId());
        } else {
            throw new IOException("Cannot serialize " + object);
        }
    }

    @Override
    public Object read(ObjectDataInput in)
            throws IOException {
        int id = in.readInt();
        return new Session(id);
    }

    @Override
    public int getTypeId() {
        return 1001;
    }

    @Override
    public void destroy() {

    }
}
