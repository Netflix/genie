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
package com.netflix.genie.web.rpc.grpc.interceptors;

import io.grpc.ForwardingServerCall;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import lombok.extern.slf4j.Slf4j;

/**
 * Proof of concept server interceptor that logs gRPC requests and errors.
 */
@Slf4j
public class SimpleLoggingInterceptor implements ServerInterceptor {

    private static final String NO_CAUSE = "Error cause is unknown";

    /**
     * {@inheritDoc}
     */
    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
        final ServerCall<ReqT, RespT> call,
        final Metadata headers,
        final ServerCallHandler<ReqT, RespT> next
    ) {

        return next.startCall(new ForwardingServerCall.SimpleForwardingServerCall<ReqT, RespT>(call) {

            /**
             * {@inheritDoc}
             */
            @Override
            public void request(final int numMessages) {
                log.info("gRPC call: {}", call.getMethodDescriptor().getFullMethodName());
                super.request(numMessages);
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public void close(final Status status, final Metadata trailers) {
                if (!status.isOk()) {
                    log.error(
                        "gRPC error: {} -> {}: {}",
                        call.getMethodDescriptor().getFullMethodName(),
                        String.valueOf(status.getCode().value()),
                        (status.getCause() != null ? status.getCause().getMessage() : NO_CAUSE),
                        status.getCause()
                    );
                }
                super.close(status, trailers);
            }
        }, headers);
    }
}
