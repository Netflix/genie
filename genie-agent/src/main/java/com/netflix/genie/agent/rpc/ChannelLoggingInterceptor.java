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

package com.netflix.genie.agent.rpc;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.MethodDescriptor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

/**
 * Interceptor that logs outbound gRPC calls.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Component
@Lazy
@Slf4j
class ChannelLoggingInterceptor implements ClientInterceptor {

    /**
     * {@inheritDoc}
     */
    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
        final MethodDescriptor<ReqT, RespT> method,
        final CallOptions callOptions,
        final Channel next
    ) {
        final String methodType = method.getType().toString();
        final String methodName = method.getFullMethodName();
        final int channelId = next.hashCode();
        log.info("gRPC {} call: {} (channel: {})", methodType, methodName, channelId);
        return next.newCall(method, callOptions);
    }
}
