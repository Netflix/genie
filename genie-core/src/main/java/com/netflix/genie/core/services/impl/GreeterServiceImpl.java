/*
 *
 *  Copyright 2016 Netflix, Inc.
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
 package com.netflix.genie.core.services.impl;

import com.netflix.genie.protogen.GreeterServiceGrpc;
import com.netflix.genie.protogen.HelloReply;
import com.netflix.genie.protogen.HelloRequest;
import io.grpc.stub.StreamObserver;
import net.devh.springboot.autoconfigure.grpc.server.GrpcService;

/**
 * Example implementation of the Greeter service definition.
 */
@GrpcService(GreeterServiceGrpc.class)
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
