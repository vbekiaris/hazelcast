/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.test.compatibility;

import com.hazelcast.nio.serialization.impl.Versioned;

import java.util.ArrayDeque;
import java.util.Queue;

/**
 * Track objects serialized in a stack
 */
public class ObjectGraphTracker {
    final Queue<Object> objects = new ArrayDeque<Object>();

    void add(Object object) {
        objects.add(object);
    }

    String validateVersioned() {
        boolean notVersionedFound = false;
        Object next = objects.poll();
        StringBuilder builder = new StringBuilder();
        while (next != null) {
            builder.append(next.getClass()).append(">");
            if (next instanceof Versioned && notVersionedFound) {
                return builder.toString();
            }
            if (!(next instanceof Versioned)) {
                notVersionedFound = true;
            }
            next = objects.poll();
        }
        System.out.println("DEBUG: " + builder.toString());
        return null;
    }

    int depth() {
        return objects.size();
    }
}
