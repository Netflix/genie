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
package com.netflix.genie.web.spring.autoconfigure.agent.apis.rpc.v4.endpoints;

import com.netflix.genie.common.internal.dtos.v4.converters.JobDirectoryManifestProtoConverter;
import com.netflix.genie.common.internal.dtos.v4.converters.JobServiceProtoConverter;
import com.netflix.genie.common.internal.util.GenieHostInfo;
import com.netflix.genie.proto.FileStreamServiceGrpc;
import com.netflix.genie.proto.HeartBeatServiceGrpc;
import com.netflix.genie.proto.JobKillServiceGrpc;
import com.netflix.genie.proto.JobServiceGrpc;
import com.netflix.genie.proto.PingServiceGrpc;
import com.netflix.genie.web.agent.apis.rpc.v4.endpoints.GRpcAgentFileStreamServiceImpl;
import com.netflix.genie.web.agent.apis.rpc.v4.endpoints.GRpcHeartBeatServiceImpl;
import com.netflix.genie.web.agent.apis.rpc.v4.endpoints.GRpcJobKillServiceImpl;
import com.netflix.genie.web.agent.apis.rpc.v4.endpoints.GRpcJobServiceImpl;
import com.netflix.genie.web.agent.apis.rpc.v4.endpoints.GRpcPingServiceImpl;
import com.netflix.genie.web.agent.apis.rpc.v4.endpoints.JobServiceProtoErrorComposer;
import com.netflix.genie.web.agent.services.AgentConnectionTrackingService;
import com.netflix.genie.web.agent.services.AgentJobService;
import com.netflix.genie.web.agent.services.AgentRoutingService;
import com.netflix.genie.web.data.services.DataServices;
import com.netflix.genie.web.properties.AgentFileStreamProperties;
import com.netflix.genie.web.properties.HeartBeatProperties;
import com.netflix.genie.web.services.RequestForwardingService;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

/**
 * Configures various gRPC services and related beans if gRPC functionality is enabled.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Configuration
@Slf4j
@EnableConfigurationProperties(
    {
        AgentFileStreamProperties.class,
        HeartBeatProperties.class,
    }
)
public class AgentRpcEndpointsAutoConfiguration {

    private static final int SINGLE_THREAD = 1;

    /**
     * Get the task scheduler used by the HeartBeat Service.
     *
     * @return The task scheduler
     */
    @Bean
    @ConditionalOnMissingBean(name = "heartBeatServiceTaskScheduler")
    public TaskScheduler heartBeatServiceTaskScheduler() {
        final ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(SINGLE_THREAD);
        return scheduler;
    }

    /**
     * Bean for converting errors in the job service to gRPC messages.
     *
     * @return An instance of {@link JobServiceProtoErrorComposer}
     */
    @Bean
    @ConditionalOnMissingBean(JobServiceProtoErrorComposer.class)
    public JobServiceProtoErrorComposer jobServiceProtoErrorComposer() {
        return new JobServiceProtoErrorComposer();
    }

    /**
     * Provide an implementation of {@link com.netflix.genie.proto.FileStreamServiceGrpc.FileStreamServiceImplBase}
     * if no other is provided.
     *
     * @param converter     The {@link JobDirectoryManifestProtoConverter} instance to use
     * @param taskScheduler The {@link TaskScheduler} to use to schedule tasks
     * @param properties    The service properties
     * @param registry      The meter registry
     * @return An instance of {@link GRpcAgentFileStreamServiceImpl}
     */
    @Bean
    @ConditionalOnMissingBean(FileStreamServiceGrpc.FileStreamServiceImplBase.class)
    public GRpcAgentFileStreamServiceImpl gRpcAgentFileStreamService(
        final JobDirectoryManifestProtoConverter converter,
        @Qualifier("genieTaskScheduler") final TaskScheduler taskScheduler,
        final AgentFileStreamProperties properties,
        final MeterRegistry registry
    ) {
        return new GRpcAgentFileStreamServiceImpl(converter, taskScheduler, properties, registry);
    }

    /**
     * Provide an implementation of {@link com.netflix.genie.proto.HeartBeatServiceGrpc.HeartBeatServiceImplBase}
     * if no other is provided.
     *
     * @param agentConnectionTrackingService The {@link AgentConnectionTrackingService} implementation to use
     * @param properties                     The service properties
     * @param taskScheduler                  The {@link TaskScheduler} instance to use
     * @param registry                       The meter registry
     * @return A {@link GRpcHeartBeatServiceImpl} instance
     */
    @Bean
    @ConditionalOnMissingBean(HeartBeatServiceGrpc.HeartBeatServiceImplBase.class)
    public GRpcHeartBeatServiceImpl gRpcHeartBeatService(
        final AgentConnectionTrackingService agentConnectionTrackingService,
        final HeartBeatProperties properties,
        @Qualifier("heartBeatServiceTaskScheduler") final TaskScheduler taskScheduler,
        final MeterRegistry registry
    ) {
        return new GRpcHeartBeatServiceImpl(agentConnectionTrackingService, properties, taskScheduler, registry);
    }

    /**
     * Provide an implementation of {@link com.netflix.genie.proto.JobKillServiceGrpc.JobKillServiceImplBase}
     * if no other is provided.
     *
     * @param dataServices             The {@link DataServices} instance to use
     * @param agentRoutingService      The {@link AgentRoutingService} instance to use to find where agents are
     *                                 connected
     * @param requestForwardingService The {@link RequestForwardingService} implementation to use to forward requests to
     *                                 other Genie nodes
     * @return A {@link GRpcJobKillServiceImpl} instance
     */
    @Bean
    @ConditionalOnMissingBean(JobKillServiceGrpc.JobKillServiceImplBase.class)
    public GRpcJobKillServiceImpl gRpcJobKillService(
        final DataServices dataServices,
        final AgentRoutingService agentRoutingService,
        final RequestForwardingService requestForwardingService
    ) {
        return new GRpcJobKillServiceImpl(dataServices, agentRoutingService, requestForwardingService);
    }

    /**
     * Provide an implementation of {@link com.netflix.genie.proto.JobServiceGrpc.JobServiceImplBase} if no other is
     * provided.
     *
     * @param agentJobService          The {@link AgentJobService} instance to use
     * @param jobServiceProtoConverter The {@link JobServiceProtoConverter} instance to use
     * @param protoErrorComposer       The {@link JobServiceProtoErrorComposer} instance to use
     * @param meterRegistry            The meter registry
     * @return A {@link GRpcJobServiceImpl} instance
     */
    @Bean
    @ConditionalOnMissingBean(JobServiceGrpc.JobServiceImplBase.class)
    public GRpcJobServiceImpl gRpcJobService(
        final AgentJobService agentJobService,
        final JobServiceProtoConverter jobServiceProtoConverter,
        final JobServiceProtoErrorComposer protoErrorComposer,
        final MeterRegistry meterRegistry
    ) {
        return new GRpcJobServiceImpl(agentJobService, jobServiceProtoConverter, protoErrorComposer, meterRegistry);
    }

    /**
     * Provide an implementation of {@link com.netflix.genie.proto.PingServiceGrpc.PingServiceImplBase} if no
     * other is provided.
     *
     * @param genieHostInfo The information about the Genie host
     * @return Instance of {@link GRpcPingServiceImpl}
     */
    @Bean
    @ConditionalOnMissingBean(PingServiceGrpc.PingServiceImplBase.class)
    public GRpcPingServiceImpl gRpcPingService(final GenieHostInfo genieHostInfo) {
        return new GRpcPingServiceImpl(genieHostInfo);
    }
}
