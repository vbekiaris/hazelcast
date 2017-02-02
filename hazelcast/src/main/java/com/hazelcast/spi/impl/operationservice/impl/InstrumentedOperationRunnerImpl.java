/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import brave.Span;
import brave.Tracer;
import brave.sampler.CountingSampler;
import com.hazelcast.internal.util.counters.Counter;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.Operation;
import zipkin.Endpoint;
import zipkin.reporter.AsyncReporter;
import zipkin.reporter.urlconnection.URLConnectionSender;

import java.net.UnknownHostException;

import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;
import static com.hazelcast.spi.impl.operationservice.impl.ZipkinUtil.forAddress;

/**
 * Responsible for processing an Operation.
 */
@SuppressWarnings("checkstyle:classfanoutcomplexity")
class InstrumentedOperationRunnerImpl
        extends OperationRunnerImpl {

    public static final String TRACING_SERVICE_NAME = "operation-service";

    private final Tracer tracer;
    private final ThreadLocal<Span> executionSpan = new ThreadLocal<Span>();
    private final ThreadLocal<Span> childSpan = new ThreadLocal<Span>();

    public InstrumentedOperationRunnerImpl(OperationServiceImpl operationService, int partitionId,
                                           Counter failedBackupsCounter) {
        super(operationService, partitionId, failedBackupsCounter);
        Tracer.Builder tracerBuilder = Tracer.newBuilder();
        Endpoint.Builder endpointBuilder = null;
        endpointBuilder = forAddress(operationService.node.address);
        endpointBuilder.port(operationService.node.address.getPort());
        tracerBuilder.localEndpoint(endpointBuilder.serviceName(TRACING_SERVICE_NAME).build())
                     .sampler(CountingSampler.create(0.1f))
                     .reporter(AsyncReporter.builder(
                             URLConnectionSender.create("http://localhost:9411/api/v1/spans"))
                                .build());
        this.tracer = tracerBuilder.build();
    }

    @Override
    protected void onBeforeDeserialize(Packet packet) {
        if (executionSpan.get() != null) {
            executionSpan.get().annotate("stale").finish();
        }
        executionSpan.set(tracer.newTrace()
                                .tag("partition-id", Integer.toString(packet.getPartitionId()))
                                .start());
        childSpan.set(tracer.newChild(executionSpan.get().context()).name("deserialize").start());
    }

    @Override
    protected void onAfterDeserialize(Operation operation) {
        if (childSpan.get() != null) {
            childSpan.get().finish();
        }
        executionSpan.get().name("remote: " + operation.getClass().getName())
                            .remoteEndpoint(ZipkinUtil.forAddress(operation.getCallerAddress())
                                            .serviceName("operation-service").build());
    }

    @Override
    protected void onDeserializationException(long callId, Packet packet) {
        if (childSpan.get() != null) {
            childSpan.get().annotate("operation-deserialization-error");
            finished();
        }
    }

    @Override
    protected void onInvalidMember(Operation operation) {
        executionSpan.get().annotate("invalid-member").finish();
        finished();
    }

    @Override
    protected void onBeforeOperationExecution(Operation operation) {
        if (executionSpan.get() == null) {
            executionSpan.set(tracer.newTrace()
                                    .name("local: " + operation.getClass().getName())
                                    .start());
        }
        childSpan.set(tracer.newChild(executionSpan.get().context()).name("execute").start());
        childSpan.get().tag("call-id", Long.toString(operation.getCallId()))
                     .tag("caller-uuid", operation.getCallerUuid())
                     .tag("operation", operation.getClass().toString());
    }

    @Override
    protected void onOperationResponseSent(Operation op) {
        childSpan.get().annotate("response-sent");
    }

    @Override
    protected void onOperationFinished(Operation op) {
        finished();
    }

    @Override
    protected void onOperationException(Operation operation, Throwable e) {
        executionSpan.get().tag("exception", e.getClass().toString());
    }

    private void finished() {
        childSpan.get().finish();
        executionSpan.get().finish();
        executionSpan.set(null);
    }
}
