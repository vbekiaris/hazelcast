/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.internal.serialization.impl;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Objects;

import static com.hazelcast.internal.serialization.SerializationService.ROOT_CONTEXT_ID;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SerializerRegistrationTest {

    private static final String SESSION_CLASS_NAME =
            "com.hazelcast.internal.serialization.impl.SerializerRegistrationTest$Session";
    private static final String SESSION_SERIALIZER_NAME =
            "com.hazelcast.internal.serialization.impl.SerializerRegistrationTest$SessionSerializer";
    private static final int NON_DEFAULT_CONTEXT_ID = 1;

    @Rule
    public ExpectedException expected = ExpectedException.none();

    private InternalSerializationService serializationService;

    @Before
    public void setup() {
        serializationService = new DefaultSerializationServiceBuilder().build();
    }

    @Test(expected = HazelcastSerializationException.class)
    public void testSessionNotSerializableByDefault() {
        Session session = new Session(1);
        serializationService.toData(session);
    }

    @Test
    public void testSerializerRegistrationInRootContext() {
        serializationService.registerSerializer(ROOT_CONTEXT_ID,
                SESSION_CLASS_NAME, SESSION_SERIALIZER_NAME);
        Session session = new Session(1);
        assertEquals(session, serializationService.toObject(serializationService.toData(session)));
    }

    @Test
    public void testSerializerRegistrationInOtherContext() {
        serializationService.registerSerializer(NON_DEFAULT_CONTEXT_ID, SESSION_CLASS_NAME, SESSION_SERIALIZER_NAME);
        Session session = new Session(1);
        assertEquals(session, serializationService.toObject(NON_DEFAULT_CONTEXT_ID,
                serializationService.toData(NON_DEFAULT_CONTEXT_ID, session)));
    }

    @Test
    public void testSerializerRegistrationInRootContext_serializationFromOtherContext() {
        serializationService.registerSerializer(ROOT_CONTEXT_ID, SESSION_CLASS_NAME, SESSION_SERIALIZER_NAME);
        Session session = new Session(1);
        assertEquals(session, serializationService.toObject(NON_DEFAULT_CONTEXT_ID,
                serializationService.toData(NON_DEFAULT_CONTEXT_ID, session)));
    }

    @Test(expected = HazelcastSerializationException.class)
    public void testSerializerRegistrationInOtherContext_serializationFromRootContext() {
        serializationService.registerSerializer(NON_DEFAULT_CONTEXT_ID, SESSION_CLASS_NAME, SESSION_SERIALIZER_NAME);
        Session session = new Session(1);
        serializationService.toData(ROOT_CONTEXT_ID, session);
    }

    @Test
    public void testUnregister() {
        serializationService.registerSerializer(ROOT_CONTEXT_ID, SESSION_CLASS_NAME, SESSION_SERIALIZER_NAME);
        Session session = new Session(1);
        serializationService.toData(session);
        serializationService.unregisterSerializer(ROOT_CONTEXT_ID, SESSION_CLASS_NAME);

        expected.expect(HazelcastSerializationException.class);
        serializationService.toData(session);
    }

    public static class Session {
        private int id;

        public Session() {
        }

        public Session(int id) {
            this.id = id;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        @Override
        public String toString() {
            return "Session{" + "id=" + id + '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Session session = (Session) o;
            return id == session.id;
        }

        @Override
        public int hashCode() {
            return Objects.hash(id);
        }
    }

    public static class SessionSerializer implements StreamSerializer {

        @Override
        public void write(ObjectDataOutput out, Object object)
                throws IOException {
            if (object instanceof Session) {
                out.writeInt(((Session) object).id);
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
}
