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

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

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

    ExecCommand(final ExecCommandArguments execCommandArguments) {
        this.execCommandArguments = execCommandArguments;
    }

    @Override
    public void run() {
        try {
            // TODO placeholder for actual job launch...
            log.info("Executing job...");
            Thread.sleep(execCommandArguments.getJobTimeout());
            log.info("Job completed");

        } catch (final InterruptedException e) {
            log.warn("Interrupted during job execution");
            Thread.currentThread().interrupt();
        }
    }

    @Component
    @Parameters(commandNames = "exec", commandDescription = "Execute a Genie job")
    @Getter
    static class ExecCommandArguments implements AgentCommandArguments {
        @Parameter(names = "timeout", description = "Job execution timeout")
        private int jobTimeout = 2000;

        @Override
        public Class<? extends AgentCommand> getConsumerClass() {
            return ExecCommand.class;
        }
    }
}
