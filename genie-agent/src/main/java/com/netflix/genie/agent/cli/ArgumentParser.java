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

import com.beust.jcommander.JCommander;
import com.netflix.genie.agent.execution.CleanupStrategy;
import lombok.extern.slf4j.Slf4j;

import java.util.Set;

/**
 * Command-line parser for the Genie Agent executable.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
class ArgumentParser {

    private final JCommander jCommander;
    private final CommandFactory commandFactory;
    private final MainCommandArguments mainCommandArguments;

    ArgumentParser(
        final JCommander jCommander,
        final CommandFactory commandFactory,
        final MainCommandArguments mainCommandArguments
    ) {
        this.jCommander = jCommander;
        this.commandFactory = commandFactory;
        this.mainCommandArguments = mainCommandArguments;
    }

    /**
     * Parse command-line arguments.
     *
     * @param args command-line arguments
     */
    void parse(final String[] args) {
        final String[] optionArguments = Util.getOptionArguments(args);
        final String[] operandArguments = Util.getOperandArguments(args);

        jCommander.parse(optionArguments);
        mainCommandArguments.set(operandArguments);
    }

    /**
     * Get a formatted string with all known options and sub-commands.
     *
     * @return the usage message string
     */
    String getUsageMessage() {
        final StringBuilder stringBuilder = new StringBuilder();
        jCommander.getUsageFormatter().usage(stringBuilder);

        stringBuilder
            .append("\n\n")
            .append(CleanupStrategy.CLEANUP_HELP_MESSAGE)
            .append("\n")
            .append(ArgumentConverters.CriterionConverter.CRITERION_SYNTAX_MESSAGE)
            .append("\n")
            .append(ArgumentConverters.UriOrLocalPathConverter.ATTACHMENT_HELP_MESSAGE)
            .append("\n")
            .append(ExitCode.EXIT_CODE_HELP_MESSAGE)
            .append("\n");

        return stringBuilder.toString();
    }

    /**
     * Get the name of the command selected via arguments.
     *
     * @return the name of a command selected or null
     */
    String getSelectedCommand() {
        return jCommander.getParsedCommand();
    }

    Set<String> getCommandNames() {
        return commandFactory.getCommandNames();
    }
}
