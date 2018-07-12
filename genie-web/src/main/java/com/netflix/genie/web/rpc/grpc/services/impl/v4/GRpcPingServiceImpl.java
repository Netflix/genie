/*
 *
 *  Copyright 2018 Netflix, Inc.
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
 package com.netflix.genie.web.rpc.grpc.services.impl.v4;

import com.google.protobuf.util.Timestamps;
import com.netflix.genie.common.internal.util.GenieHostInfo;
import com.netflix.genie.proto.PingRequest;
import com.netflix.genie.proto.PingServiceGrpc;
import com.netflix.genie.proto.PongResponse;
import com.netflix.genie.web.properties.GRpcServerProperties;
import com.netflix.genie.web.rpc.grpc.interceptors.SimpleLoggingInterceptor;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import net.devh.springboot.autoconfigure.grpc.server.GrpcService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

import java.util.Map;

/**
 * Implementation of the Ping service definition.
 *
 * @author mprimi
 * @since 4.0.0
 */
@ConditionalOnProperty(value = GRpcServerProperties.ENABLED_PROPERTY, havingValue = "true")
@GrpcService(
    value = PingServiceGrpc.class,
    interceptors = {
        SimpleLoggingInterceptor.class,
    }
)
@Slf4j
public class GRpcPingServiceImpl extends PingServiceGrpc.PingServiceImplBase {

    private final String hostName;

    GRpcPingServiceImpl(final GenieHostInfo genieHostInfo) {
        this.hostName = genieHostInfo.getHostname();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void ping(
        final PingRequest request,
        final StreamObserver<PongResponse> responseObserver
    ) {
        final PongResponse response = PongResponse.newBuilder()
            .setRequestId(request.getRequestId())
            .setTimestamp(Timestamps.fromMillis(System.currentTimeMillis()))
            .putServerMetadata(ServerMetadataKeys.SERVER_NAME, hostName)
            .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();

        final StringBuilder sb = new StringBuilder();

        sb.append(
            String.format(
                "Received ping with id: '%s' from client: '%s' and timestamp: '%s'. ",
                request.getRequestId(),
                request.getSourceName(),
                Timestamps.toString(request.getTimestamp())
            )
        );

        sb.append("Client metadata: [ ");
        for (final Map.Entry<String, String> clientMetadataEntry : request.getClientMetadataMap().entrySet()) {
            sb
                .append("{")
                .append(clientMetadataEntry.getKey())
                .append(" : ")
                .append(clientMetadataEntry.getValue())
                .append("}, ");
        }
        sb.append("]");

        log.info(sb.toString());
    }

    static final class ServerMetadataKeys {
        static final String SERVER_NAME = "hostName";
    }
}

