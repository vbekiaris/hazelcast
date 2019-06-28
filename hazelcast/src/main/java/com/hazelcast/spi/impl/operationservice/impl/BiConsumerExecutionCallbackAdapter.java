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

package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.core.ExecutionCallback;

import java.util.function.BiConsumer;

// TODO to be removed, temporary adapter to initially avoid rewriting all ExecutionCallbacks
public class BiConsumerExecutionCallbackAdapter<V> implements BiConsumer<V, Throwable> {

    private final ExecutionCallback<V> executionCallback;

    public BiConsumerExecutionCallbackAdapter(ExecutionCallback<V> executionCallback) {
        this.executionCallback = executionCallback;
    }

    @Override
    public void accept(V value, Throwable throwable) {
        if (throwable == null) {
            executionCallback.onResponse(value);
        } else {
            executionCallback.onFailure(throwable);
        }
    }
}
