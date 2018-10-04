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
package com.netflix.genie.agent.execution.services.impl.grpc;

import com.netflix.genie.agent.execution.services.AgentHeartBeatService;
import com.netflix.genie.agent.execution.services.AgentJobKillService;
import com.netflix.genie.agent.execution.services.AgentJobService;
import com.netflix.genie.agent.execution.services.KillService;
import com.netflix.genie.common.internal.dto.v4.converters.JobServiceProtoConverter;
import com.netflix.genie.proto.HeartBeatServiceGrpc;
import com.netflix.genie.proto.JobKillServiceGrpc;
import com.netflix.genie.proto.JobServiceGrpc;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.TaskScheduler;

/**
 * Spring auto configuration for the various gRPC services required for an agent to communicate with the Genie server.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Configuration
public class GRpcServicesAutoConfiguration {

    /**
     * Provide a lazy gRPC agent heart beat service if one isn't already defined.
     *
     * @param heartBeatServiceStub The heart beat service stub to use
     * @param taskScheduler        The task scheduler to use
     * @return A {@link GrpcAgentHeartBeatServiceImpl} instance
     */
    @Bean
    @Lazy
    @ConditionalOnMissingBean(AgentHeartBeatService.class)
    public AgentHeartBeatService agentHeartBeatService(
        final HeartBeatServiceGrpc.HeartBeatServiceStub heartBeatServiceStub,
        @Qualifier("heartBeatServiceTaskExecutor") final TaskScheduler taskScheduler
    ) {
        return new GrpcAgentHeartBeatServiceImpl(heartBeatServiceStub, taskScheduler);
    }

    /**
     * Provide a lazy gRPC agent job kill service bean if one isn't already defined.
     *
     * @param jobKillServiceFutureStub The future stub to use for the service communication with the server
     * @param killService              The kill service to use to terminate this agent gracefully
     * @param taskExecutor             The task executor to use
     * @return A {@link GRpcAgentJobKillServiceImpl} instance
     */
    @Bean
    @Lazy
    @ConditionalOnMissingBean(AgentJobKillService.class)
    public AgentJobKillService agentJobKillService(
        final JobKillServiceGrpc.JobKillServiceFutureStub jobKillServiceFutureStub,
        final KillService killService,
        @Qualifier("sharedAgentTaskExecutor") final TaskExecutor taskExecutor
    ) {
        return new GRpcAgentJobKillServiceImpl(jobKillServiceFutureStub, killService, taskExecutor);
    }

    /**
     * Provide a lazy gRPC agent job service bean if one isn't already defined.
     *
     * @param jobServiceFutureStub     The future stub to use for communication with the server
     * @param jobServiceProtoConverter The converter to use between DTO and Proto instances
     * @return A {@link GRpcAgentJobServiceImpl} instance
     */
    @Bean
    @Lazy
    @ConditionalOnMissingBean(AgentJobService.class)
    public AgentJobService agentJobService(
        final JobServiceGrpc.JobServiceFutureStub jobServiceFutureStub,
        final JobServiceProtoConverter jobServiceProtoConverter
    ) {
        return new GRpcAgentJobServiceImpl(jobServiceFutureStub, jobServiceProtoConverter);
    }
}
