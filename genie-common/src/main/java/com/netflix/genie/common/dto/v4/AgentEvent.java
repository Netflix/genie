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

package com.netflix.genie.common.dto.v4;

import com.google.common.collect.Lists;
import com.netflix.genie.common.dto.JobStatus;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import javax.annotation.Nullable;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.time.Instant;
import java.util.Collections;
import java.util.List;

/**
 * AgentEvent DTOs.
 * @author mprimi
 * @since 4.0.0
 */
@Getter
@EqualsAndHashCode(doNotUseGetters = true)
@ToString(doNotUseGetters = true)
public abstract class AgentEvent {
    @NotBlank
    private final String agentId;
    @NotNull
    private final Instant timestamp;

    private AgentEvent(
        @NotBlank final String agentId,
        final Instant... timestamp
    ) {
        this.agentId = agentId;
        if (timestamp.length == 0) {
            this.timestamp = Instant.now();
        } else if (timestamp.length > 1) {
            throw new IllegalArgumentException("Expecting 1 optional argument: timestamp, got " + timestamp.length);
        } else {
            this.timestamp = timestamp[0];
        }
    }

    /**
     * Event fired when a running job changes status.
     */
    @Getter
    @EqualsAndHashCode(doNotUseGetters = true, callSuper = true)
    @ToString(doNotUseGetters = true, callSuper = true)
    public static class JobStatusUpdate extends AgentEvent {

        @NotBlank
        private final String jobId;
        @NotNull
        private final JobStatus jobStatus;

        /**
         * Constructor.
         * @param agentId agent id
         * @param jobId job id
         * @param jobStatus current job status
         * @param timestamp optional timestamp
         */
        public JobStatusUpdate(
            final @NotBlank String agentId,
            final @NotBlank String jobId,
            final JobStatus jobStatus,
            final Instant... timestamp
        ) {
            super(agentId, timestamp);
            this.jobId = jobId;
            this.jobStatus = jobStatus;
        }
    }

    /**
     * Event fired when the agent state machine changes state.
     */
    @Getter
    @EqualsAndHashCode(doNotUseGetters = true, callSuper = true)
    @ToString(doNotUseGetters = true, callSuper = true)
    public static class StateChange extends AgentEvent {

        @Nullable @NotEmpty
        private final String fromState;
        @NotBlank
        private final String toState;

        /**
         * Constructor.
         * @param agentId agent id
         * @param fromState previous state
         * @param toState next state
         * @param timestamp optional timestamp
         */
        public StateChange(
            final @NotBlank String agentId,
            final @Nullable @NotEmpty String fromState,
            final @NotBlank String toState,
            final Instant... timestamp
        ) {
            super(agentId, timestamp);
            this.fromState = fromState;
            this.toState = toState;
        }
    }

    /**
     * Event fired an agent state machine state action is executed (successfully or otherwise).
     */
    @Getter
    @EqualsAndHashCode(doNotUseGetters = true, callSuper = true)
    @ToString(doNotUseGetters = true, callSuper = true)
    public static class StateActionExecution extends AgentEvent {

        private final @NotBlank String state;
        private final @NotBlank String action;
        private final boolean actionException;
        private final @Nullable String exceptionClass;
        private final @Nullable String exceptionMessage;
        private final @Nullable List<@NotBlank String> exceptionTrace;

        /**
         * Constructor.
         * @param agentId agent id
         * @param state state name
         * @param action action class name
         * @param timestamp optional timestamp
         */
        public StateActionExecution(
            final @NotBlank String agentId,
            final @NotBlank String state,
            final @NotBlank String action,
            final Instant... timestamp
        ) {
            super(agentId, timestamp);
            this.state = state;
            this.action = action;
            this.exceptionMessage = null;
            this.exceptionClass = null;
            this.exceptionTrace = null;
            this.actionException = false;
        }

        /**
         * Constructor.
         * @param agentId agent id
         * @param state state name
         * @param action action class name
         * @param exception exception thrown by action
         * @param timestamp optional timestamp
         */
        public StateActionExecution(
            final @NotBlank String agentId,
            final @NotBlank String state,
            final @NotBlank String action,
            final Exception exception,
            final Instant... timestamp
        ) {
            this(
                agentId,
                state,
                action,
                exception.getClass().getCanonicalName(),
                exception.getMessage(),
                getExceptionTrace(exception),
                timestamp
            );
        }

        /**
         * Constructor.
         * @param agentId agent id
         * @param state state name
         * @param action action class name
         * @param exceptionClass class of exception thrown by action
         * @param exceptionMessage message of exception thrown by action
         * @param exceptionTrace a list of descriptions of causes for the exception thrown by the action
         * @param timestamp optional timestamp
         */
        public StateActionExecution(
            final @NotBlank String agentId,
            final @NotBlank String state,
            final @NotBlank String action,
            final @NotBlank String exceptionClass,
            final @NotBlank String exceptionMessage,
            final @NotEmpty List<@NotBlank String> exceptionTrace,
            final Instant... timestamp
        ) {
            super(agentId, timestamp);
            this.state = state;
            this.action = action;
            this.exceptionClass = exceptionClass;
            this.exceptionMessage = exceptionMessage;
            this.exceptionTrace = exceptionTrace;
            this.actionException = true;
        }

        private static List<String> getExceptionTrace(final Exception exception) {
            final List<String> trace = Lists.newArrayList();
            Throwable currentException = exception;
            while (currentException != null) {
                trace.add(
                    String.format(
                        "%s : %s",
                        currentException.getClass().getCanonicalName(),
                        currentException.getMessage())
                );
                currentException = currentException.getCause();
            }
            return Collections.unmodifiableList(trace);
        }
    }
}
