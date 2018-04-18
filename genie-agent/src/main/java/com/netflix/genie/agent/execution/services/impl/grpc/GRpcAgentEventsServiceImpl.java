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

import com.google.common.collect.Queues;
import com.netflix.genie.agent.execution.ExecutionContext;
import com.netflix.genie.agent.execution.services.AgentEventsService;
import com.netflix.genie.agent.execution.statemachine.States;
import com.netflix.genie.agent.execution.statemachine.actions.StateAction;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.dto.v4.AgentEvent;
import com.netflix.genie.proto.AgentEventRequest;
import com.netflix.genie.proto.AgentEventResponse;
import com.netflix.genie.proto.AgentEventsServiceGrpc;
import com.netflix.genie.proto.v4.converters.AgentEventConverter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;
import org.springframework.validation.annotation.Validated;

import javax.annotation.Nullable;
import javax.annotation.PreDestroy;
import java.util.Queue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of AgentEventsService that delivers events to the server asynchronously over gRPC.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Component
@Lazy
@Slf4j
@Validated
class GRpcAgentEventsServiceImpl implements AgentEventsService {

    private static final long RETRY_BACKOFF_MILLIS = 1000; //TODO: make this configurable
    private static final long QUEUE_DRAIN_MAX_WAIT_MILLIS = 10000;  //TODO: make this configurable
    private static final long EXECUTOR_SHUTDOWN_MAX_WAIT_MILLIS = 3000; //TODO: make this configurable
    private final ExecutionContext executionContext;
    private final Queue<AgentEventRequest> eventQueue;
    private final ScheduledExecutorService executor;
    private final ScheduledFuture<?> scheduledFuture;
    private final AgentEventConverter agentEventConverter;

    GRpcAgentEventsServiceImpl(
        final ExecutionContext executionContext,
        final AgentEventsServiceGrpc.AgentEventsServiceBlockingStub client,
        final AgentEventConverter agentEventConverter
    ) {
        this.executionContext = executionContext;
        this.agentEventConverter = agentEventConverter;
        this.eventQueue = Queues.newLinkedBlockingQueue();
        this.executor = new ScheduledThreadPoolExecutor(1);
        this.scheduledFuture = this.executor.scheduleWithFixedDelay(
            new DeliverEventsRunnable(client, eventQueue),
            0,
            RETRY_BACKOFF_MILLIS,
            TimeUnit.MILLISECONDS
        );
    }

    /**
     * Graceful shutdown, allow some time to drain the event queue.
     * @throws Exception in case of error
     */
    @PreDestroy
    public void cleanUp() throws Exception {

        log.info("Shutting down");

        final long shutdownDeadline = System.currentTimeMillis() + QUEUE_DRAIN_MAX_WAIT_MILLIS;

        synchronized (eventQueue) {
            while (System.currentTimeMillis() < shutdownDeadline && !eventQueue.isEmpty()) {
                eventQueue.wait(100);
            }
            log.debug("Event queue {} within the allocated time", eventQueue.isEmpty() ? "drained" : "not drained");
        }

        scheduledFuture.cancel(true);
        executor.shutdown();

        try {
            executor.awaitTermination(EXECUTOR_SHUTDOWN_MAX_WAIT_MILLIS, TimeUnit.MILLISECONDS);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            log.debug("Interrupted while waiting for executor termination");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void emitJobStatusUpdate(final JobStatus jobStatus) {
        enqueueEvent(
            agentEventConverter.toProto(
                new AgentEvent.JobStatusUpdate(
                    executionContext.getAgentId(),
                    executionContext.getJobSpecification().getJob().getId(),
                    jobStatus
                )
            )
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void emitStateChange(@Nullable final States fromState, final States toState) {
        enqueueEvent(
            agentEventConverter.toProto(
                new AgentEvent.StateChange(
                    executionContext.getAgentId(),
                    fromState == null ? null : fromState.name(),
                    toState.name()
                )
            )
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void emitStateActionExecution(final States state, final StateAction action) {
        enqueueEvent(
            agentEventConverter.toProto(
                new AgentEvent.StateActionExecution(
                    executionContext.getAgentId(),
                    state.name(),
                    action.getClass().getCanonicalName()
                )
            )
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void emitStateActionExecution(
        final States state,
        final StateAction actionClass,
        final Exception exception
    ) {
        enqueueEvent(
            agentEventConverter.toProto(
                new AgentEvent.StateActionExecution(
                    executionContext.getAgentId(),
                    state.name(),
                    actionClass.getClass().getCanonicalName(),
                    exception
                )
            )
        );
    }

    private void enqueueEvent(final AgentEventRequest agentEvent) {
        final int queueSize;
        synchronized (eventQueue) {
            eventQueue.add(agentEvent);
            eventQueue.notify();
            queueSize = eventQueue.size();
        }
        log.debug("Events in queue: {}", queueSize);
    }

    private static final class DeliverEventsRunnable implements Runnable {
        private final AgentEventsServiceGrpc.AgentEventsServiceBlockingStub client;
        private final Queue<AgentEventRequest> eventQueue;

        DeliverEventsRunnable(
            final AgentEventsServiceGrpc.AgentEventsServiceBlockingStub client,
            final Queue<AgentEventRequest> eventQueue
        ) {
            this.client = client;
            this.eventQueue = eventQueue;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void run() {
            while (true) {
                final AgentEventRequest nextEvent;
                synchronized (eventQueue) {
                    // Wait for queue not empty
                    while (eventQueue.isEmpty()) {
                        try {
                            log.debug("Queue is empty, waiting");
                            eventQueue.wait();
                        } catch (final InterruptedException e) {
                            Thread.currentThread().interrupt();
                            log.warn("Interrupted while waiting for events to deliver");
                            return;
                        }
                    }
                    nextEvent = eventQueue.peek();
                }

                log.debug("Delivering next event");
                final AgentEventResponse acknowledgement;
                try {
                    acknowledgement = client.publishEvent(nextEvent);
                } catch (final Exception e) {
                    log.warn("Failed to deliver event to server, will retry later", e);
                    return;
                }

                if (acknowledgement == null) {
                    log.warn("Failed to deliver event to server, will retry later");
                    return;
                }

                log.debug("Event acknowledged");
                synchronized (eventQueue) {
                    eventQueue.remove();
                    // Notify for shutdown thread
                    eventQueue.notify();
                }
            }
        }
    }
}
