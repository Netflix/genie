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

package com.netflix.genie.agent.execution.statemachine.actions;

import com.netflix.genie.agent.execution.ExecutionContext;
import com.netflix.genie.agent.execution.exceptions.InvalidStateException;
import com.netflix.genie.agent.execution.statemachine.Events;
import com.netflix.genie.agent.execution.statemachine.States;
import com.netflix.genie.common.dto.JobStatus;
import lombok.extern.slf4j.Slf4j;
import org.springframework.statemachine.StateContext;

import java.util.Optional;

/**
 * Base class for StateAction.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
public abstract class BaseStateAction implements StateAction {

    private final ExecutionContext executionContext;

    protected BaseStateAction(final ExecutionContext executionContext) {
        this.executionContext = executionContext;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void execute(final StateContext<States, Events> context) {

        final States currentState = context.getStateMachine().getState().getId();
        final String currentActionName = this.getClass().getSimpleName();

        // Enqueue this action for later cleanup
        executionContext.addCleanupActions(this);

        Events nextEvent;
        try {
            log.info("Performing state {} validation before action: {}", currentState, currentActionName);
            executePreActionValidation();
            log.info("Performing state {} action: {}", currentState, currentActionName);
            // State action returns the next event to send (or null)
            nextEvent = executeStateAction(executionContext);
            log.info("State action {} returned {} as next event", currentActionName, nextEvent);
            log.info("Performing state {} validation after action: {}", currentState, currentActionName);
            executePostActionValidation();
        } catch (final Exception e) {
            executionContext.addStateActionError(currentState, this.getClass(), e);
            nextEvent = Events.ERROR;
            log.error(
                "Action {} failed with exception",
                currentActionName,
                e
            );
        }

        final boolean accepted = context.getStateMachine().sendEvent(nextEvent);
        if (!accepted) {
            log.warn(
                "State machine in state {} rejected event {}",
                currentState,
                nextEvent
            );
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void cleanup() {
        final String currentActionName = this.getClass().getSimpleName();
        log.info("Executing cleanup for action {}", currentActionName);
        executeStateActionCleanup(executionContext);
    }

    protected abstract void executePreActionValidation();

    protected abstract Events executeStateAction(ExecutionContext ctx);

    protected abstract void executePostActionValidation();

    protected void executeStateActionCleanup(final ExecutionContext ctx) {
        log.debug("Action has no cleanup");
    }

    void assertCurrentJobStatusEqual(final JobStatus expectedJobStatus) {
        final Optional<JobStatus> optionalJobStatus = executionContext.getCurrentJobStatus();
        if (!optionalJobStatus.isPresent()) {
            throw new InvalidStateException("Job status not in context");
        }

        final JobStatus jobStatus = optionalJobStatus.get();
        if (!expectedJobStatus.equals(jobStatus)) {
            throw new InvalidStateException(
                String.format("Job is in status: %s (expected: %s", String.valueOf(jobStatus), expectedJobStatus)
            );
        }
    }

    void assertClaimedJobIdPresent() {
        assertPresent(executionContext.getClaimedJobId(), "claimedJobId");
    }

    private void assertPresent(final Optional<?> optional, final String name) {
        if (!optional.isPresent()) {
            throw new InvalidStateException(
                String.format("Context attribute %s is not set", name)
            );
        }
    }

    private void assertNotPresent(final Optional<?> optional, final String name) {
        if (optional.isPresent()) {
            throw new InvalidStateException(
                String.format("Context attribute %s is set", name)
            );
        }
    }

    void assertJobSpecificationPresent() {
        assertPresent(executionContext.getJobSpecification(), "jobSpecification");
    }

    void assertJobDirectoryPresent() {
        assertPresent(executionContext.getJobDirectory(), "jobDirectory");
    }

    void assertJobEnvironmentPresent() {
        assertPresent(executionContext.getJobEnvironment(), "jobEnvironment");
    }

    void assertFinalJobStatusPresent() {
        assertPresent(executionContext.getFinalJobStatus(), "finalJobStatus");
    }

    void assertFinalJobStatusPresentAndValid() {
        assertFinalJobStatusPresent();
        final JobStatus finalJobStatus = executionContext.getFinalJobStatus().get();
        if (!finalJobStatus.isFinished()) {
            throw new InvalidStateException(
                String.format("Final job status is not a final status: %s", finalJobStatus.name())
            );
        }
        assertCurrentJobStatusEqual(finalJobStatus);
    }

    void assertClaimedJobIdNotPresent() {
        assertNotPresent(executionContext.getClaimedJobId(), "claimedJobId");
    }

    void assertCurrentJobStatusNotPresent() {
        assertNotPresent(executionContext.getCurrentJobStatus(), "currentJobStatus");
    }

    void assertJobSpecificationNotPresent() {
        assertNotPresent(executionContext.getJobSpecification(), "jobSpecification");
    }

    void assertJobDirectoryNotPresent() {
        assertNotPresent(executionContext.getJobDirectory(), "jobDirectory");
    }

    void assertJobEnvironmentNotPresent() {
        assertNotPresent(executionContext.getJobEnvironment(), "jobEnvironment");
    }

    void assertFinalJobStatusNotPresent() {
        assertNotPresent(executionContext.getFinalJobStatus(), "finalJobStatus");
    }

    void assertCurrentJobStatusPresentIfJobIdPresent() {
        final Optional<String> optionalJobId = executionContext.getClaimedJobId();
        final Optional<JobStatus> optionalJobStatus = executionContext.getCurrentJobStatus();
        if (optionalJobId.isPresent() && !optionalJobStatus.isPresent()) {
            throw new InvalidStateException("Context has a job id, but job status is unknown");
        }
    }

    void assertFinalJobStatusPresentAndValidIfJobIdPresent() {
        final Optional<String> optionalJobId = executionContext.getClaimedJobId();
        final Optional<JobStatus> optionalJobStatus = executionContext.getFinalJobStatus();
        if (optionalJobId.isPresent() && !optionalJobStatus.isPresent()) {
            throw new InvalidStateException("Context has a job id, but final job status is unknown");
        }
        if (optionalJobStatus.isPresent()) {
            assertFinalJobStatusPresentAndValid();
        }
    }
}
