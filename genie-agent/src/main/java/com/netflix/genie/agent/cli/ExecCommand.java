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
package com.netflix.genie.agent.cli;

import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import com.google.common.annotations.VisibleForTesting;
import com.netflix.genie.agent.cli.logging.ConsoleLog;
import com.netflix.genie.agent.execution.services.KillService;
import com.netflix.genie.agent.execution.statemachine.ExecutionContext;
import com.netflix.genie.agent.execution.statemachine.FatalJobExecutionException;
import com.netflix.genie.agent.execution.statemachine.JobExecutionStateMachine;
import com.netflix.genie.common.external.dtos.v4.JobStatus;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Command to execute a Genie job.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
class ExecCommand implements AgentCommand {

    private final ExecCommandArguments execCommandArguments;
    private final JobExecutionStateMachine stateMachine;
    private final KillService killService;
    private final List<sun.misc.Signal> signalsToIntercept = Collections.unmodifiableList(Arrays.asList(
        new sun.misc.Signal("INT"),
        new sun.misc.Signal("TERM")
    ));

    ExecCommand(
        final ExecCommandArguments execCommandArguments,
        final JobExecutionStateMachine stateMachine,
        final KillService killService
    ) {
        this.execCommandArguments = execCommandArguments;
        this.stateMachine = stateMachine;
        this.killService = killService;
    }

    @Override
    public ExitCode run() {
        for (final sun.misc.Signal s : signalsToIntercept) {
            sun.misc.Signal.handle(s, signal -> handleTerminationSignal());
        }

        log.info("Starting job execution");
        try {
            this.stateMachine.run();
        } catch (final Exception e) {
            log.error("Job state machine execution failed: {}", e.getMessage());
            throw e;
        }

        final ExecutionContext executionContext = this.stateMachine.getExecutionContext();

        final JobStatus finalJobStatus = executionContext.getCurrentJobStatus();
        final boolean jobLaunched = executionContext.isJobLaunched();

        final FatalJobExecutionException fatalException = executionContext.getExecutionAbortedFatalException();

        if (fatalException != null) {
            ConsoleLog.getLogger().error(
                "Job execution fatal error in state {}: {}",
                fatalException.getSourceState(),
                fatalException.getCause().getMessage()
            );
        }

        final ExitCode exitCode;

        switch (finalJobStatus) {
            case SUCCEEDED:
                log.info("Job executed successfully");
                exitCode = ExitCode.SUCCESS;
                break;
            case KILLED:
                log.info("Job killed during execution");
                exitCode = ExitCode.EXEC_ABORTED;
                break;
            case FAILED:
                if (jobLaunched) {
                    log.info("Job execution failed");
                    exitCode = ExitCode.EXEC_FAIL;
                } else {
                    log.info("Job setup failed");
                    exitCode = ExitCode.COMMAND_INIT_FAIL;
                }
                break;
            case INVALID:
                log.info("Job execution initialization failed");
                exitCode = ExitCode.INIT_FAIL;
                break;
            default:
                throw new RuntimeException("Unexpected final job status: " + finalJobStatus.name());
        }

        return exitCode;
    }

    @VisibleForTesting
    void handleTerminationSignal() {
        ConsoleLog.getLogger().info("Kill requested, terminating job");
        log.warn("Received kill signal");
        this.killService.kill(KillService.KillSource.SYSTEM_SIGNAL);
    }

    @Parameters(commandNames = CommandNames.EXEC, commandDescription = "Execute a Genie job")
    @Getter
    static class ExecCommandArguments implements AgentCommandArguments {
        @ParametersDelegate
        private final ArgumentDelegates.ServerArguments serverArguments;

        @ParametersDelegate
        private final ArgumentDelegates.CacheArguments cacheArguments;

        @ParametersDelegate
        private final ArgumentDelegates.JobRequestArguments jobRequestArguments;

        @ParametersDelegate
        private final ArgumentDelegates.CleanupArguments cleanupArguments;

        @ParametersDelegate
        private final ArgumentDelegates.RuntimeConfigurationArguments runtimeConfigurationArguments;

        ExecCommandArguments(
            final ArgumentDelegates.ServerArguments serverArguments,
            final ArgumentDelegates.CacheArguments cacheArguments,
            final ArgumentDelegates.JobRequestArguments jobRequestArguments,
            final ArgumentDelegates.CleanupArguments cleanupArguments,
            final ArgumentDelegates.RuntimeConfigurationArguments runtimeConfigurationArguments
        ) {
            this.serverArguments = serverArguments;
            this.cacheArguments = cacheArguments;
            this.jobRequestArguments = jobRequestArguments;
            this.cleanupArguments = cleanupArguments;
            this.runtimeConfigurationArguments = runtimeConfigurationArguments;
        }

        @Override
        public Class<? extends AgentCommand> getConsumerClass() {
            return ExecCommand.class;
        }
    }
}
