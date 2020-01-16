/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.avro.impl;

import com.hazelcast.avro.SchemaRegistry;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ReplicatedMap;
import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SchemaRegistryImpl implements SchemaRegistry {

    HazelcastInstance instance;

    public SchemaRegistryImpl(HazelcastInstance instance) {
        this.instance = instance;
    }

    @Override
    public void add(Schema schema) {
        map().put(schema.getName(), schema.toString());
    }

    ReplicatedMap<String, String> map() {
        return instance.getReplicatedMap(ReplicatedMapSchemaRegistry.AVRO_SCHEMAS_MAP);
    }

    public List<Schema> list() {
        Schema.Parser parser = new Schema.Parser();
        List<Schema> schemas = new ArrayList<>();
        for (Map.Entry<String, String> entry : map().entrySet()) {
            Schema schema = parser.parse(entry.getValue());
            schemas.add(schema);
        }
        return schemas;
    }
}
