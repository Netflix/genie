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
package com.netflix.genie.grpc.services.impl;

import com.netflix.genie.protogen.GreeterServiceGrpc;
import com.netflix.genie.protogen.HelloReply;
import com.netflix.genie.protogen.HelloRequest;
import com.netflix.genie.test.categories.UnitTest;
import io.grpc.testing.GrpcServerRule;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Unit tests for the class GreeterServiceImpl using in-memory client/server channel.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Category(UnitTest.class)
public class GreeterServiceImplUnitTest {

    /**
     * Create in-process client and server.
     */
    @Rule
    public final GrpcServerRule grpcServerRule = new GrpcServerRule().directExecutor();

    private GreeterServiceGrpc.GreeterServiceBlockingStub blockingStub;

    /**
     * Test set up.
     */
    @Before
    public void setUp() {
        grpcServerRule.getServiceRegistry().addService(new GreeterServiceImpl());
        blockingStub = GreeterServiceGrpc.newBlockingStub(grpcServerRule.getChannel());
    }

    /**
     * Test the response to a valid sayHello request.
     * @throws Exception in case of error
     */
    @Test
    public void sayHello() throws Exception {

        final String testName = this.getClass().getCanonicalName();

        final HelloReply reply = blockingStub.sayHello(HelloRequest.newBuilder().setName(testName).build());

        Assert.assertEquals("Hello " + testName + "!", reply.getMessage());
    }

}
