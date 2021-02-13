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
import com.netflix.genie.agent.properties.AgentProperties;
import com.netflix.genie.agent.properties.ShutdownProperties;
import com.netflix.genie.common.external.dtos.v4.JobStatus;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

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
    private final ThreadFactory threadFactory;
    private final ShutdownProperties shutdownProperties;
    private final ReentrantLock isRunningLock = new ReentrantLock();
    private final Condition isRunningCondition = this.isRunningLock.newCondition();
    private boolean isRunning;

    ExecCommand(
        final ExecCommandArguments execCommandArguments,
        final JobExecutionStateMachine stateMachine,
        final KillService killService,
        final AgentProperties agentProperties
    ) {
        this(
            execCommandArguments,
            stateMachine,
            killService,
            agentProperties,
            Thread::new
        );
    }

    @VisibleForTesting
    ExecCommand(
        final ExecCommandArguments execCommandArguments,
        final JobExecutionStateMachine stateMachine,
        final KillService killService,
        final AgentProperties agentProperties,
        final ThreadFactory threadFactory // For testing shutdown hooks
    ) {
        this.execCommandArguments = execCommandArguments;
        this.stateMachine = stateMachine;
        this.killService = killService;
        this.shutdownProperties = agentProperties.getShutdown();
        this.threadFactory = threadFactory;
    }

    @Override
    public ExitCode run() {

        // Lock-free since the only other thread accessing this has not been registered yet
        this.isRunning = true;

        // Before execution starts, add shutdown hooks
        Runtime.getRuntime().addShutdownHook(this.threadFactory.newThread(this::waitForCleanShutdown));
        Runtime.getRuntime().addShutdownHook(this.threadFactory.newThread(this::handleSystemSignal));

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

        this.isRunningLock.lock();
        try {
            this.isRunning = false;
            this.isRunningCondition.signalAll();
        } finally {
            this.isRunningLock.unlock();
        }

        return exitCode;
    }

    /**
     * This code runs when the JVM is about to shut down.
     * There are 2 different scenarios:
     * 1) Execution is completed -- Nothing to do in this case.
     * 2) A signal (TERM/HUP/INT) was received -- Job execution should be stopped in this case.
     */
    private void handleSystemSignal() {
        final boolean shouldAbortExecution;
        this.isRunningLock.lock();
        try {
            shouldAbortExecution = this.isRunning;
        } finally {
            this.isRunningLock.unlock();
        }

        if (shouldAbortExecution) {
            ConsoleLog.getLogger().info("Aborting job execution");
            killService.kill(KillService.KillSource.SYSTEM_SIGNAL);
        }
    }

    /**
     * This code runs when the JVM is about to shut down.
     * It keeps the process alive until the agent has had a chance to shut down cleanly (whatever that means, i.e.
     * success, failure, kill, ...) in order to for example archive logs, update job status, etc.
     */
    private void waitForCleanShutdown() {
        // Don't hold off shutdown indefinitely
        final long maxWaitSeconds = shutdownProperties.getExecutionCompletionLeeway().getSeconds();
        final Instant waitDeadline = Instant.now().plusSeconds(maxWaitSeconds);

        this.isRunningLock.lock();
        try {
            while (this.isRunning) {
                ConsoleLog.getLogger().info("Waiting for shutdown...");
                if (Instant.now().isAfter(waitDeadline)) {
                    log.error("Execution did not complete in the allocated time");
                    return;
                }

                try {
                    this.isRunningCondition.await(1, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    log.warn("Interrupted while waiting execution completion");
                }
            }
        } finally {
            this.isRunningLock.unlock();
            log.info("Unblocking shutdown ({})", this.isRunning ? "still running" : "completed");
        }
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
