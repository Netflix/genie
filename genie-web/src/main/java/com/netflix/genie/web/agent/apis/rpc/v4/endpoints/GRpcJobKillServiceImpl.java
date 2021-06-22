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
package com.netflix.genie.web.agent.apis.rpc.v4.endpoints;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.common.external.dtos.v4.JobStatus;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieInvalidStatusException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieJobNotFoundException;
import com.netflix.genie.proto.JobKillRegistrationRequest;
import com.netflix.genie.proto.JobKillRegistrationResponse;
import com.netflix.genie.proto.JobKillServiceGrpc;
import com.netflix.genie.web.agent.services.AgentRoutingService;
import com.netflix.genie.web.data.services.DataServices;
import com.netflix.genie.web.data.services.PersistenceService;
import com.netflix.genie.web.exceptions.checked.NotFoundException;
import com.netflix.genie.web.services.JobKillService;
import com.netflix.genie.web.services.RequestForwardingService;
import io.grpc.stub.StreamObserver;
import io.grpc.stub.ServerCallStreamObserver;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.scheduling.annotation.Scheduled;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implementation of {@link JobKillService} which uses parked gRPC requests to tell the agent to
 * shutdown via a user kill request if the job is in an active state.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Slf4j
public class GRpcJobKillServiceImpl extends JobKillServiceGrpc.JobKillServiceImplBase implements JobKillService {

    @VisibleForTesting
    @Getter(AccessLevel.PACKAGE)
    private final Map<String, StreamObserver<JobKillRegistrationResponse>> parkedJobKillResponseObservers;
    private final PersistenceService persistenceService;
    private final AgentRoutingService agentRoutingService;
    private final RequestForwardingService requestForwardingService;

    /**
     * Constructor.
     *
     * @param dataServices             The {@link DataServices} instance to use
     * @param agentRoutingService      The {@link AgentRoutingService} instance to use to find where agents are
     *                                 connected
     * @param requestForwardingService The service to use to forward requests to other Genie nodes
     */
    public GRpcJobKillServiceImpl(
        final DataServices dataServices,
        final AgentRoutingService agentRoutingService,
        final RequestForwardingService requestForwardingService
    ) {
        this.persistenceService = dataServices.getPersistenceService();
        this.parkedJobKillResponseObservers = new ConcurrentHashMap<>();
        this.agentRoutingService = agentRoutingService;
        this.requestForwardingService = requestForwardingService;
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
        final StreamObserver<JobKillRegistrationResponse> existingObserver = this.parkedJobKillResponseObservers.put(
            request.getJobId(),
            responseObserver
        );

        // If a previous observer/request is present close that request
        if (existingObserver != null) {
            existingObserver.onCompleted();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Retryable(
        value = {
            GenieInvalidStatusException.class,
            GenieServerException.class
        },
        backoff = @Backoff(delay = 1000)
    )
    public void killJob(
        final String jobId,
        final String reason,
        @Nullable final HttpServletRequest request
    ) throws GenieJobNotFoundException, GenieServerException {
        final JobStatus currentJobStatus;
        try {
            currentJobStatus = this.persistenceService.getJobStatus(jobId);
        } catch (final NotFoundException e) {
            throw new GenieJobNotFoundException(e);
        }

        if (currentJobStatus.isFinished()) {
            log.info("Job {} was already finished when the kill request arrived. Nothing to do.", jobId);
        } else if (JobStatus.getStatusesBeforeClaimed().contains(currentJobStatus)) {
            // Agent hasn't come up and claimed job yet. Setting to killed should prevent agent from starting
            try {
                this.persistenceService.updateJobStatus(jobId, currentJobStatus, JobStatus.KILLED, reason);
            } catch (final GenieInvalidStatusException e) {
                // This is the case where somewhere else in the system the status was changed before we could kill
                // Should retry entire method as job may have transitioned to a finished state
                log.error(
                    "Unable to set job status for {} to {} due to current status not being expected {}",
                    jobId,
                    JobStatus.KILLED,
                    currentJobStatus
                );
                throw e;
            } catch (final NotFoundException e) {
                throw new GenieJobNotFoundException(e);
            }
        } else if (currentJobStatus.isActive()) {
            // If we get here the Job should not currently be regarded as finished AND an agent has come up and
            // connected to one of the Genie servers, possibly this one
            if (this.agentRoutingService.isAgentConnectionLocal(jobId)) {
                // Agent should be connected here so we should have a response observer to use
                final StreamObserver<JobKillRegistrationResponse> responseObserver
                    = this.parkedJobKillResponseObservers.remove(jobId);

                if (responseObserver == null) {
                    log.error("Job {} not killed. Expected local agent connection not found", jobId);
                    throw new GenieServerException(
                        "Job " + jobId + " not killed. Expected local agent connection not found."
                    );
                }
                responseObserver.onNext(JobKillRegistrationResponse.newBuilder().build());
                responseObserver.onCompleted();

                log.info("Agent notified for killing job {}", jobId);
            } else {
                // Agent is running somewhere else try to forward the request
                final String hostname = this.agentRoutingService
                    .getHostnameForAgentConnection(jobId)
                    .orElseThrow(
                        // Note: this should retry as we may have hit a case where agent is transitioning nodes
                        //       it is connected to
                        () -> new GenieServerException(
                            "Unable to locate host where agent is connected for job " + jobId
                        )
                    );
                log.info(
                    "Agent for job {} currently connected to {}. Attempting to forward kill request",
                    jobId,
                    hostname
                );
                this.requestForwardingService.kill(hostname, jobId, request);
            }
        } else {
            // The job is in some unknown state throw exception that forces method to try again
            log.error("{} is an unhandled state for job {}", currentJobStatus, jobId);
            throw new GenieServerException(
                "Job " + jobId + " is currently in " + currentJobStatus + " status, which isn't currently handled"
            );
        }
    }

    /**
     * Remove orphaned kill observers from local map.
     * <p>
     * The logic as currently implemented is to have the Agent, once handshake is complete, open a connection to
     * the server which results in parking a response observer in the map stored in this implementation. Upon receiving
     * a kill request for the correct job this class will use the observer to send the "response" to the agent which
     * will begin shut down process. The issue is that if the agent disconnects from this server the server will never
     * realize it's gone and these observers will build up in the map in memory forever. This method will periodically
     * go through the map and determine if the observers are still valid and remove any that aren't.
     *
     * @see "GRpcAgentJobKillServiceImpl"
     */
    @Scheduled(fixedDelay = 30_000L, initialDelay = 30_000L)
    public void cleanupOrphanedObservers() {
        for (final String jobId : this.parkedJobKillResponseObservers.keySet()) {
            try {
                if (!this.agentRoutingService.isAgentConnectionLocal(jobId)) {
                    final StreamObserver<JobKillRegistrationResponse> observer = this.parkedJobKillResponseObservers
                        .remove(jobId);

                    cancelObserverIfNecessary(observer);
                }
            } catch (final Exception unexpectedException) {
                log.error("Got unexpected exception while trying to cleanup jobID {}. Moving on. "
                    + "Exception: {}", jobId, unexpectedException);
            }
        }
    }

    /**
     * Converts StreamObserver into ServerCallStreamObserver in order to tell
     * whether the observer is cancelled or not.
     *
     * @param observer Observer for which we would check the status
     * @return         Boolean value: true if observer has status CANCELLED
     */
    @VisibleForTesting
    protected boolean isStreamObserverCancelled(final StreamObserver<JobKillRegistrationResponse> observer) {
        return ((ServerCallStreamObserver<JobKillRegistrationResponse>) observer).isCancelled();
    }

    /**
     * If observer is null or already cancelled - do nothing.
     * Otherwise call onCompleted.
     * @param observer
     */
    private void cancelObserverIfNecessary(final StreamObserver<JobKillRegistrationResponse> observer) {
        if (observer != null && !isStreamObserverCancelled(observer)) {
            try {
                observer.onCompleted();
            } catch (final Exception observerException) {
                log.error("Got exception while trying to complete streamObserver during cleanup"
                    + "for jobID {}. Exception: {}", "jobId", observerException);
            }
        }
    }
}
