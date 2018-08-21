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

import com.google.common.collect.Maps;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.proto.JobKillRegistrationRequest;
import com.netflix.genie.proto.JobKillRegistrationResponse;
import com.netflix.genie.proto.JobKillServiceGrpc;
import com.netflix.genie.web.rpc.grpc.interceptors.SimpleLoggingInterceptor;
import com.netflix.genie.web.services.JobKillServiceV4;
import com.netflix.genie.web.services.JobSearchService;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import net.devh.springboot.autoconfigure.grpc.server.GrpcService;

import javax.validation.constraints.NotBlank;
import java.util.Map;

/**
 * Service to kill agent jobs.
 * TODO Register with HeartBeatService to listen for agent stream to become inactive and clean it up.
 *
 * @author standon
 * @since 4.0.0
 */
@GrpcService(
    value = JobKillServiceGrpc.class,
    interceptors = {
        SimpleLoggingInterceptor.class,
    }
)
@Slf4j
public class GRpcJobKillServiceImpl
    extends JobKillServiceGrpc.JobKillServiceImplBase
    implements JobKillServiceV4 {

    private final Map<String, StreamObserver<JobKillRegistrationResponse>> parkedJobKillResponseObservers =
        Maps.newConcurrentMap();
    private final JobSearchService jobSearchService;

    /**
     * Constructor.
     *
     * @param jobSearchService Job search service
     */
    public GRpcJobKillServiceImpl(final JobSearchService jobSearchService) {
        this.jobSearchService = jobSearchService;
    }

    /**
     * Register to be notified when a kill request for the job is received.
     *
     * @param request          Request to register for getting notified when server gets a job kill request.
     * @param responseObserver The response observer
     */
    @Override
    public void registerForKillNotification(
        final JobKillRegistrationRequest request,
        final StreamObserver<JobKillRegistrationResponse> responseObserver
    ) {
        parkedJobKillResponseObservers.put(request.getJobId(), responseObserver);
    }

    /**
     * Kill the job with the given id if possible.
     *
     * @param jobId  id of job to kill
     * @param reason brief reason for requesting the job be killed
     * @throws GenieServerException in case there is no response observer
     *                              found to communicate with the agent
     */
    @Override
    public void killJob(final @NotBlank(message = "No job id entered. Unable to kill job.") String jobId,
                        final @NotBlank(message = "No reason provided.") String reason) throws GenieException {

        final StreamObserver<JobKillRegistrationResponse> responseObserver =
            parkedJobKillResponseObservers.remove(jobId);

        if (responseObserver == null) {
            log.error("Job not killed. No response observer found for killing the job with id: {} ", jobId);
            throw new GenieServerException(
                "Job not killed. No response observer found for killing the job with id: " + jobId);
        }

        if (jobSearchService.getJobStatus(jobId).isFinished()) {
            log.info("v4 job {} was already finished when the kill request came", jobId);
        } else { //Non null response observer and an unfinished job. Send a kill to the agent
            responseObserver.onNext(
                JobKillRegistrationResponse.newBuilder().build()
            );
            responseObserver.onCompleted();

            log.info("Agent notified for killing job {}", jobId);
        }
    }
}
