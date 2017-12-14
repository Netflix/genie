/*
 *
 *  Copyright 2017 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
 package com.netflix.genie.web.rpc.services.impl;

import com.netflix.genie.web.rpc.interceptors.SimpleLoggingInterceptor;
import com.netflix.genie.proto.GreeterServiceGrpc;
import com.netflix.genie.proto.HelloReply;
import com.netflix.genie.proto.HelloRequest;
import io.grpc.stub.StreamObserver;
import net.devh.springboot.autoconfigure.grpc.server.GrpcService;

/**
 * Example implementation of the Greeter service definition.
 */
@GrpcService(
    value = GreeterServiceGrpc.class,
    interceptors = {
        SimpleLoggingInterceptor.class,
    }
)
public class GreeterServiceImpl extends GreeterServiceGrpc.GreeterServiceImplBase {

    /**
     * {@inheritDoc}
     */
    @Override
    public void sayHello(final HelloRequest request, final StreamObserver<HelloReply> responseObserver) {
        final HelloReply response = HelloReply.newBuilder()
            .setMessage("Hello " + request.getName() + "!")
            .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
