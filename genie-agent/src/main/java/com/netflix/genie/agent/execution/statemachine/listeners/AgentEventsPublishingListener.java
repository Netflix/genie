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
package com.netflix.genie.agent.execution.statemachine.listeners;

import com.netflix.genie.agent.execution.ExecutionContext;
import com.netflix.genie.agent.execution.services.AgentEventsService;
import com.netflix.genie.agent.execution.statemachine.Events;
import com.netflix.genie.agent.execution.statemachine.States;
import com.netflix.genie.common.dto.JobStatus;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Lazy;
import org.springframework.statemachine.state.State;
import org.springframework.stereotype.Component;

import javax.annotation.Nullable;

/**
 * Listener that publishes events related to job state changes.
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
@Component
@Lazy
public class AgentEventsPublishingListener extends JobExecutionListenerAdapter {

    private final ExecutionContext executionContext;
    private final AgentEventsService agentEventsService;
    private JobStatus previousJobStatus;

    AgentEventsPublishingListener(
        final ExecutionContext executionContext,
        final AgentEventsService agentEventsService
    ) {
        this.executionContext = executionContext;
        this.agentEventsService = agentEventsService;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stateChanged(@Nullable final State<States, Events> from, final State<States, Events> to) {
        agentEventsService.emitStateChange(
            from == null ? null : from.getId(),
            to.getId()
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stateEntered(final State<States, Events> state) {
        final JobStatus jobStatus = mapStateToStatus(state.getId());
        if (jobStatus != null) {
            agentEventsService.emitJobStatusUpdate(jobStatus);
            previousJobStatus = jobStatus;
        }
    }


    private @Nullable JobStatus mapStateToStatus(final States state) {
        final JobStatus jobStatus;

        if (States.SETUP_JOB == state && previousJobStatus == null) {
            jobStatus = JobStatus.INIT;
        } else if (States.MONITOR_JOB == state && JobStatus.INIT == previousJobStatus) {
            jobStatus = JobStatus.RUNNING;
        } else if (
            States.HANDLE_ERROR == state
                && (JobStatus.INIT == previousJobStatus || JobStatus.RUNNING == previousJobStatus)) {
            jobStatus = JobStatus.FAILED;
        } else if (States.CLEANUP_JOB.equals(state)) {
            jobStatus = executionContext.getFinalJobStatus();
        } else {
            jobStatus = null;
        }

        return jobStatus;
    }
}
