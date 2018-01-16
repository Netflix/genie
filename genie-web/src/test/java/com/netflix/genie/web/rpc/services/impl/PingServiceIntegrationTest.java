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
package com.netflix.genie.web.rpc.services.impl;

import com.netflix.genie.proto.PingRequest;
import com.netflix.genie.proto.PingServiceGrpc;
import com.netflix.genie.proto.PongResponse;
import com.netflix.genie.test.categories.IntegrationTest;
import io.grpc.testing.GrpcServerRule;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.UUID;

/**
 * Unit tests for the class PingServiceImpl using in-memory client/server channel.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Category(IntegrationTest.class)
public class PingServiceIntegrationTest {

    private static final String HOST_NAME = "genie-foo.netflix.com";
    /**
     * Create in-process client and server.
     */
    @Rule
    public final GrpcServerRule grpcServerRule = new GrpcServerRule().directExecutor();

    private PingServiceGrpc.PingServiceBlockingStub blockingStub;

    /**
     * Test set up.
     */
    @Before
    public void setUp() {
        grpcServerRule.getServiceRegistry().addService(new PingServiceImpl(HOST_NAME));
        blockingStub = PingServiceGrpc.newBlockingStub(grpcServerRule.getChannel());
    }

    /**
     * Test the response to a valid PingRequest.
     * @throws Exception in case of error
     */
    @Test
    public void ping() throws Exception {

        final String requestId = UUID.randomUUID().toString();

        final PingRequest request = PingRequest.newBuilder()
            .setRequestId(requestId)
            .setSourceName(this.getClass().getCanonicalName())
            .putClientMetadata("foo", "bar")
            .putClientMetadata("bar", "baz")
            .build();

        final PongResponse response = blockingStub.ping(request);

        Assert.assertEquals(requestId, response.getRequestId());
        Assert.assertNotNull(response.getTimestamp());
        Assert.assertEquals(
            HOST_NAME,
            response.getServerMetadataOrThrow(PingServiceImpl.ServerMetadataKeys.SERVER_NAME)
        );
    }

}
