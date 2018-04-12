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

package com.netflix.genie.proto.v4.converters;

import com.google.common.collect.Lists;
import com.google.protobuf.util.Timestamps;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.proto.AgentEventRequest;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

/**
 * Convert AgentEvent DTOs from and to proto messages
 * @author mprimi
 * @since 4.0.0
 */
public class AgentEventConverter {

    public com.netflix.genie.common.dto.v4.AgentEvent toDTO(final AgentEventRequest agentEventRequest) {

        final String agentId = agentEventRequest.getAgentId();
        final Instant timestamp = Instant.ofEpochMilli(Timestamps.toMillis(agentEventRequest.getTimestamp()));

        switch (agentEventRequest.getEventTypeCase()) {
            case JOB_STATUS_UPDATE_EVENT:
                return toDTO(agentId, timestamp, agentEventRequest.getJobStatusUpdateEvent());

            case STATE_CHANGE_EVENT:
                return toDTO(agentId, timestamp, agentEventRequest.getStateChangeEvent());

            case STATE_ACTION_EXECUTION_EVENT:
                return toDTO(agentId, timestamp, agentEventRequest.getStateActionExecutionEvent());

            default:
                throw new IllegalArgumentException(
                    "Request does not contain an known event" + agentEventRequest.toString()
                );
        }
    }

    public com.netflix.genie.common.dto.v4.AgentEvent.JobStatusUpdate toDTO(
        final String agentId,
        final Instant timestamp,
        final AgentEventRequest.JobStatusUpdate jobStatusUpdate
    ) {
        try {
            return new com.netflix.genie.common.dto.v4.AgentEvent.JobStatusUpdate(
                agentId,
                jobStatusUpdate.getJobId(),
                JobStatus.parse(jobStatusUpdate.getJobStatus()),
                timestamp
            );
        } catch (final GeniePreconditionException e) {
            throw new IllegalArgumentException("Unrecognized JobStatus: " + jobStatusUpdate.getJobStatus(), e);
        }
    }

    public com.netflix.genie.common.dto.v4.AgentEvent.StateChange toDTO(
        final String agentId,
        final Instant timestamp,
        final AgentEventRequest.StateChange stateChange
    ) {
        return new com.netflix.genie.common.dto.v4.AgentEvent.StateChange(
            agentId,
            stateChange.getFromState().isEmpty() ? null : stateChange.getFromState(),
            stateChange.getToState(),
            timestamp
        );
    }

    public com.netflix.genie.common.dto.v4.AgentEvent.StateActionExecution toDTO(
        final String agentId,
        final Instant timestamp,
        final AgentEventRequest.StateActionExecution stateActionExecution
    ) {
        if (stateActionExecution.hasActionException()) {
            final AgentEventRequest.StateActionExecution.ActionException actionException
                = stateActionExecution.getActionException();
            return new com.netflix.genie.common.dto.v4.AgentEvent.StateActionExecution(
                agentId,
                stateActionExecution.getStateName(),
                stateActionExecution.getActionName(),
                actionException.getExceptionClass(),
                actionException.getExceptionMessage(),
                Lists.newArrayList(actionException.getExceptionTraceList()),
                timestamp
            );
        } else {
            return new com.netflix.genie.common.dto.v4.AgentEvent.StateActionExecution(
                agentId,
                stateActionExecution.getStateName(),
                stateActionExecution.getActionName(),
                timestamp
            );
        }
    }

    public AgentEventRequest toProto(
        final com.netflix.genie.common.dto.v4.AgentEvent.JobStatusUpdate event
    ) {

        final AgentEventRequest.JobStatusUpdate jobStatusUpdateEvent = AgentEventRequest.JobStatusUpdate.newBuilder()
            .setJobStatus(event.getJobStatus().name())
            .setJobId(event.getJobId())
            .build();

        return getAgentEventBuilder(event)
            .setJobStatusUpdateEvent(jobStatusUpdateEvent)
            .build();
    }

    public AgentEventRequest toProto(
        final com.netflix.genie.common.dto.v4.AgentEvent.StateChange event
    ) {

        final AgentEventRequest.StateChange stateChangeEvent = AgentEventRequest.StateChange.newBuilder()
            .setFromState(event.getFromState() != null ? event.getFromState() : "")
            .setToState(event.getToState())
            .build();

        return getAgentEventBuilder(event)
            .setStateChangeEvent(stateChangeEvent)
            .build();
    }

    public AgentEventRequest toProto(
        final com.netflix.genie.common.dto.v4.AgentEvent.StateActionExecution event
    ) {

        final AgentEventRequest.StateActionExecution.Builder actionExecutionEventBuilder
            = AgentEventRequest.StateActionExecution.newBuilder()
            .setStateName(event.getState())
            .setActionName(event.getAction());

        if (event.isActionException()) {
            actionExecutionEventBuilder.setActionException(
                AgentEventRequest.StateActionExecution.ActionException.newBuilder()
                    .setExceptionClass(event.getExceptionClass())
                    .setExceptionMessage(event.getExceptionMessage())
                    .addAllExceptionTrace(event.getExceptionTrace())
            );
        }

        return getAgentEventBuilder(event)
            .setStateActionExecutionEvent(actionExecutionEventBuilder.build())
            .build();
    }


    private AgentEventRequest.Builder getAgentEventBuilder(final com.netflix.genie.common.dto.v4.AgentEvent event) {
        return AgentEventRequest.newBuilder()
            .setAgentId(event.getAgentId())
            .setTimestamp(
                Timestamps.fromNanos(
                TimeUnit.NANOSECONDS.convert(event.getTimestamp().getEpochSecond(), TimeUnit.SECONDS) +
                    event.getTimestamp().getNano()
                )
            );
    }

}
