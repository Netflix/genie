/*
 *
 *  Copyright 2019 Netflix, Inc.
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

import com.google.protobuf.Empty;
import com.netflix.genie.agent.execution.services.AgentFileManifestService;
import com.netflix.genie.common.internal.dto.JobDirectoryManifest;
import com.netflix.genie.common.internal.dto.v4.converters.JobDirectoryManifestProtoConverter;
import com.netflix.genie.common.internal.exceptions.GenieConversionException;
import com.netflix.genie.common.internal.util.ExponentialBackOffTrigger;
import com.netflix.genie.proto.JobFileManifestMessage;
import com.netflix.genie.proto.JobFileServiceGrpc;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.TaskScheduler;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.ScheduledFuture;

/**
 * Implementation of {@link AgentFileManifestService} that publishes the manifest over gRPC.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
public class GRpcAgentFileManifestServiceImpl implements AgentFileManifestService {
    private static final boolean ENABLE_COMPRESSION = true; //TODO make configurable

    private final JobFileServiceGrpc.JobFileServiceStub jobFileServiceStub;
    private final TaskScheduler taskScheduler;
    private final ExponentialBackOffTrigger trigger;
    private final JobDirectoryManifestProtoConverter manifestProtoConverter;
    private StreamObserver<JobFileManifestMessage> manifestStreamObserver;
    private String jobId;
    private Path jobDirectoryPath;
    private boolean started;
    private ScheduledFuture<?> scheduledTask;
    private final StreamObserver<Empty> responseObserver;

    GRpcAgentFileManifestServiceImpl(
        final JobFileServiceGrpc.JobFileServiceStub jobFileServiceStub,
        final TaskScheduler taskScheduler,
        final JobDirectoryManifestProtoConverter manifestProtoConverter
    ) {
        this.jobFileServiceStub = jobFileServiceStub;
        this.taskScheduler = taskScheduler;
        this.manifestProtoConverter = manifestProtoConverter;
        this.trigger = new ExponentialBackOffTrigger(
            ExponentialBackOffTrigger.DelayType.FROM_PREVIOUS_EXECUTION_BEGIN,
            1000, //TODO make configurable
            10000, //TODO make configurable
            1.1f //TODO make configurable
        );
        this.responseObserver = new ManifestResponseObserver(this);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void start(final String claimedJobId, final Path jobDirectoryRoot) {
        //Service can be started only once
        if (started) {
            throw new IllegalStateException("Service can be started only once");
        }

        this.jobId = claimedJobId;
        this.jobDirectoryPath = jobDirectoryRoot;
        this.started = true;
        this.scheduledTask = this.taskScheduler.schedule(this::pushManifest, trigger);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void stop() {
        this.scheduledTask.cancel(false);
        this.scheduledTask = null;
        this.discardCurrentStream(true);
        this.started = false;
    }

    private synchronized void pushManifest() {
        if (started) {
            final JobFileManifestMessage jobFileManifest;
            try {
                jobFileManifest = manifestProtoConverter.manifestToProtoMessage(
                    this.jobId,
                    new JobDirectoryManifest(this.jobDirectoryPath, false)
                );
            } catch (final IOException e) {
                log.error("Failed to construct manifest", e);
                return;
            } catch (GenieConversionException e) {
                log.error("Failed to serialize manifest", e);
                return;
            }

            if (this.manifestStreamObserver == null) {
                this.manifestStreamObserver = jobFileServiceStub.pushManifest(responseObserver);
                if (this.manifestStreamObserver instanceof ClientCallStreamObserver) {
                    ((ClientCallStreamObserver) this.manifestStreamObserver).setMessageCompression(ENABLE_COMPRESSION);
                }
            }

            this.manifestStreamObserver.onNext(jobFileManifest);
        }
    }

    private synchronized void discardCurrentStream(final boolean sendStreamCompletion) {
        if (this.manifestStreamObserver != null) {
            if (sendStreamCompletion) {
                this.manifestStreamObserver.onCompleted();
            }
            this.manifestStreamObserver = null;
        }
    }

    private static class ManifestResponseObserver implements StreamObserver<Empty> {
        private final GRpcAgentFileManifestServiceImpl gRpcAgentFileManifestService;

        ManifestResponseObserver(final GRpcAgentFileManifestServiceImpl gRpcAgentFileManifestService) {
            this.gRpcAgentFileManifestService = gRpcAgentFileManifestService;
        }

        @Override
        public void onNext(final Empty value) {
        }

        @Override
        public void onError(final Throwable t) {
            log.warn("Manifest stream error: {}", t.getMessage(), t);
            this.gRpcAgentFileManifestService.trigger.reset();
            this.gRpcAgentFileManifestService.discardCurrentStream(false);
        }

        @Override
        public void onCompleted() {
            log.debug("Manifest stream completed");
            this.gRpcAgentFileManifestService.discardCurrentStream(false);
        }
    }
}
