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
import com.netflix.genie.agent.execution.ExecutionContext;
import com.netflix.genie.agent.execution.services.KillService;
import com.netflix.genie.agent.execution.statemachine.JobExecutionStateMachine;
import com.netflix.genie.agent.execution.statemachine.States;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Triple;
import org.springframework.context.annotation.Lazy;
import org.springframework.statemachine.action.Action;
import org.springframework.stereotype.Component;

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
@Component
@Lazy
class ExecCommand implements AgentCommand {

    private final ExecCommandArguments execCommandArguments;
    private final JobExecutionStateMachine stateMachine;
    private final ExecutionContext executionContext;
    private final KillService killService;
    private final List<sun.misc.Signal> signalsToIntercept = Collections.unmodifiableList(Arrays.asList(
        new sun.misc.Signal("INT"),
        new sun.misc.Signal("TERM")
    ));

    ExecCommand(
        final ExecCommandArguments execCommandArguments,
        final JobExecutionStateMachine stateMachine,
        final ExecutionContext executionContext,
        final KillService killService
    ) {
        this.execCommandArguments = execCommandArguments;
        this.stateMachine = stateMachine;
        this.executionContext = executionContext;
        this.killService = killService;
    }

    @Override
    public void run() {
        for (final sun.misc.Signal s : signalsToIntercept) {
            sun.misc.Signal.handle(s, signal -> handleTerminationSignal());
        }

        log.info("Running job state machine");
        stateMachine.start();

        final States finalstate;
        try {
            finalstate = stateMachine.waitForStop();
        } catch (final Exception e) {
            log.warn("Job state machine execution failed", e);
            throw new RuntimeException("Job execution error", e);
        }

        if (!States.END.equals(finalstate)) {
            throw new RuntimeException("Job execution failed (final state: " + finalstate + ")");
        }

        if (executionContext.hasStateActionError()) {
            final List<Triple<States, Class<? extends Action>, Exception>> actionErrors
                = executionContext.getStateActionErrors();
            actionErrors.forEach(
                triple -> log.error(
                    "Action {} in state {} failed with {}: {}",
                    triple.getMiddle().getSimpleName(),
                    triple.getLeft(),
                    triple.getRight().getClass().getSimpleName(),
                    triple.getRight().getMessage()
                ));

            final Exception firstActionErrorException = actionErrors.get(0).getRight();

            throw new RuntimeException("Job execution error", firstActionErrorException);
        }

        log.info("Job execution completed successfully");

    }

    @VisibleForTesting
    void handleTerminationSignal() {
        UserConsole.getLogger().info(
            "Intercepted a signal, terminating job (status: {})",
            executionContext.getCurrentJobStatus()
        );
        killService.kill(KillService.KillSource.SYSTEM_SIGNAL);
    }

    @Component
    @Parameters(commandNames = CommandNames.EXEC, commandDescription = "Execute a Genie job")
    @Getter
    static class ExecCommandArguments implements AgentCommandArguments {
        @ParametersDelegate
        private final ArgumentDelegates.ServerArguments serverArguments;

        @ParametersDelegate
        private final ArgumentDelegates.CacheArguments cacheArguments;

        @ParametersDelegate
        private final ArgumentDelegates.JobRequestArguments jobRequestArguments;

        ExecCommandArguments(
            final ArgumentDelegates.ServerArguments serverArguments,
            final ArgumentDelegates.CacheArguments cacheArguments,
            final ArgumentDelegates.JobRequestArguments jobRequestArguments
        ) {
            this.serverArguments = serverArguments;
            this.cacheArguments = cacheArguments;
            this.jobRequestArguments = jobRequestArguments;
        }

        @Override
        public Class<? extends AgentCommand> getConsumerClass() {
            return ExecCommand.class;
        }
    }
}
