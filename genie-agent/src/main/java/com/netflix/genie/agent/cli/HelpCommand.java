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
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

/**
 * Command to print agent usage/help message.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
@Component
@Lazy
class HelpCommand implements AgentCommand {


    private final ArgumentParser argumentParser;

    HelpCommand(
        final ArgumentParser argumentParser
    ) {
        this.argumentParser = argumentParser;
    }

    @Override
    public ExitCode run() {
        System.out.println(argumentParser.getUsageMessage());

        return ExitCode.SUCCESS;
    }

    @Component
    @Parameters(commandNames = CommandNames.HELP, commandDescription = "Print agent usage and help message")
    @Getter
    static class HelpCommandArguments implements AgentCommandArguments {
        @Override
        public Class<? extends AgentCommand> getConsumerClass() {
            return HelpCommand.class;
        }
    }
}
