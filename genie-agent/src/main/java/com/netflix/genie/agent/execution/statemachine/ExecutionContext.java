/*
 *
 *  Copyright 2020 Netflix, Inc.
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
package com.netflix.genie.agent.execution.statemachine;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.netflix.genie.agent.execution.CleanupStrategy;
import com.netflix.genie.agent.execution.process.JobProcessResult;
import com.netflix.genie.agent.execution.statemachine.stages.ClaimJobStage;
import com.netflix.genie.agent.execution.statemachine.stages.ConfigureExecutionStage;
import com.netflix.genie.agent.execution.statemachine.stages.CreateJobDirectoryStage;
import com.netflix.genie.agent.execution.statemachine.stages.CreateJobScriptStage;
import com.netflix.genie.agent.execution.statemachine.stages.InitializeAgentStage;
import com.netflix.genie.agent.execution.statemachine.stages.LaunchJobStage;
import com.netflix.genie.agent.execution.statemachine.stages.ObtainJobSpecificationStage;
import com.netflix.genie.agent.execution.statemachine.stages.ReserveJobIdStage;
import com.netflix.genie.agent.execution.statemachine.stages.WaitJobCompletionStage;
import com.netflix.genie.agent.properties.AgentProperties;
import com.netflix.genie.common.external.dtos.v4.AgentClientMetadata;
import com.netflix.genie.common.external.dtos.v4.AgentJobRequest;
import com.netflix.genie.common.external.dtos.v4.JobSpecification;
import com.netflix.genie.common.external.dtos.v4.JobStatus;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.Synchronized;

import javax.validation.constraints.NotNull;
import java.io.File;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Stores runtime information that is passed from one state to the next.
 * Example, exceptions encountered during execution, whether a job process was launched or not, the local job directory,
 * if one was created, etc.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Getter(onMethod_ = {@Synchronized})
@Setter(onMethod_ = {@Synchronized})
public class ExecutionContext {

    /**
     * Whether the state machine associated to this execution context has already been started.
     */
    private final AtomicBoolean started = new AtomicBoolean(false);
    /**
     * List of all exception thrown by state transitions.
     */
    private final List<TransitionExceptionRecord> transitionExceptionRecords = Lists.newArrayList();

    /**
     * Agent properties.
     */
    private final AgentProperties agentProperties;

    /**
     * Agent client metadata sent to the server in certain requests.
     * Present if {@link InitializeAgentStage} ran successfully.
     */
    private AgentClientMetadata agentClientMetadata;
    /**
     * Job id requested by the user.
     * Present if the the jobId option is present on the command-line and the
     * {@link ConfigureExecutionStage} ran successfully.
     * May be blank/null if the the agent is not executing an "API" job.
     */
    private String requestedJobId;
    /**
     * Job id reserved for this job.
     * Present and guaranteed not to be blank if the {@link ReserveJobIdStage} ran successfully.
     */
    private String reservedJobId;
    /**
     * Job id claimed by this agent for execution.
     * Present and guaranteed not to be blank if the {@link ClaimJobStage} ran successfully.
     */
    private String claimedJobId;
    /**
     * The job status as seen by the server.
     * Present if {@link ReserveJobIdStage} ran successfully, modified as execution progresses.
     */
    private JobStatus currentJobStatus = JobStatus.INVALID;
    /**
     * The job request constructed from command-line arguments.
     * Present if {@link ConfigureExecutionStage} ran successfully and if the agent is excuting a new job specified via
     * command-line arguments (as opposed to claiming a pre-resolved job submitted via API).
     */
    private AgentJobRequest agentJobRequest;
    /**
     * True if the agent is executing a job that was submitted via API and is pre-resolved on the server.
     * False if the agent is creating and executing a new job based on command-line arguments.
     */
    private boolean isPreResolved;
    /**
     * The job specification obtained from the server.
     * Present if {@link ObtainJobSpecificationStage} ran successfully.
     */
    private JobSpecification jobSpecification;
    /**
     * The local job directory created to execute a job.
     * Present if {@link CreateJobDirectoryStage} ran successfully.
     */
    private File jobDirectory;
    /**
     * The local job script file.
     * Present if {@link CreateJobScriptStage} ran successfully.
     */
    private File jobScript;
    /**
     * True if the job process should be launched from within the job directory.
     * False if the job should be launched from the current directory.
     */
    private boolean isRunFromJobDirectory;
    /**
     * True if the job process was launched (in {@link LaunchJobStage}), false if execution was aborted before reaching
     * that stage.
     */
    private boolean jobLaunched;
    /**
     * The result (based on exit code) of launching the job process.
     * Present if the job was launched and
     * {@link WaitJobCompletionStage} ran successfully.
     */
    private JobProcessResult jobProcessResult;
    /**
     * True if at any point a request was received to kill the job.
     */
    private boolean isJobKilled;
    /**
     * Current tally of attempts left for a given state transition.
     * Reset to 1 (or bigger) when each new state is reached.
     * Can be zeroed if an attempt results in a fatal error.
     */
    private int attemptsLeft;
    /**
     * An exception that caused the execution to be aborted.
     * Present if a critical stage encountered a fatal exception (either thrown by the transition, or due to exhaustion
     * of attempts, or due to unhandled exception.
     */
    private FatalJobExecutionException executionAbortedFatalException;

    /**
     * The kind of cleanup to perform post-execution.
     * Present if {@link ConfigureExecutionStage} ran successfully.
     */
    @NotNull
    private CleanupStrategy cleanupStrategy = CleanupStrategy.DEFAULT_STRATEGY;

    /**
     * The next job status that should be send to the server.
     */
    @NotNull
    private JobStatus nextJobStatus = JobStatus.INVALID;

    /**
     * The message to attach to when the job status is updated.
     */
    private String nextJobStatusMessage;

    /**
     * The state machine that executes the job.
     */
    private JobExecutionStateMachine stateMachine;

    /**
     * Whether to skip sending the final job status to the server.
     * Used when the job is shut down because the remote status was already set to FAILED by the leader.
     */
    private boolean skipFinalStatusUpdate;

    /**
     * Constructor.
     *
     * @param agentProperties The agent properties
     */
    public ExecutionContext(final AgentProperties agentProperties) {
        this.agentProperties = agentProperties;
    }

    /**
     * Record an execution exception, even if it is retryable.
     *
     * @param state             the state in which the exception occurred
     * @param recordedException the exception
     */
    public void recordTransitionException(final States state, final Exception recordedException) {
        this.transitionExceptionRecords.add(
            new TransitionExceptionRecord(state, recordedException)
        );
    }

    /**
     * Get the list of execution exception encountered (fatal and non-fatal).
     *
     * @return a list of exception records
     */
    public List<TransitionExceptionRecord> getTransitionExceptionRecords() {
        return ImmutableList.copyOf(this.transitionExceptionRecords);
    }

    /**
     * Convenience method to determine whether the execution is aborted (as a result of a fatal error in a critical
     * state or due to a kill request).
     *
     * @return whether the execution is aborted.
     */
    public boolean isExecutionAborted() {
        return this.executionAbortedFatalException != null || this.isJobKilled;
    }

    /**
     * Data object that contains an exception and the state in which it was encountered.
     */
    @AllArgsConstructor
    @Getter
    public static class TransitionExceptionRecord {
        private final States state;
        private final Exception recordedException;
    }
}
