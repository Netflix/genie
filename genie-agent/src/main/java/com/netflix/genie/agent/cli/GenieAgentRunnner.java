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

import com.beust.jcommander.ParameterException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.ExitCodeGenerator;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Set;

/**
 * Main entry point for execution after the application is initialized.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
@Component
public class GenieAgentRunnner implements CommandLineRunner, ExitCodeGenerator {

    private final ArgumentParser argumentParser;
    private final CommandFactory commandFactory;
    private ExitCode exitCode = ExitCode.INIT_FAIL;

    GenieAgentRunnner(
        final ArgumentParser argumentParser,
        final CommandFactory commandFactory
    ) {
        this.argumentParser = argumentParser;
        this.commandFactory = commandFactory;
    }

    @Override
    public void run(final String... args) throws Exception {
        try {
            internalRun(args);
        } catch (final Throwable t) {
            final Throwable userConsoleException = t.getCause() != null ? t.getCause() : t;
            UserConsole.getLogger().error(
                "Command execution failed: {}",
                userConsoleException.getMessage(),
                userConsoleException
            );

            UserConsole.getLogger().info("Full execution log file: {}", UserConsole.getLogFilePath());
            log.info("Command execution failed", t);
        }
    }

    private void internalRun(final String[] args) {
        log.info("Parsing arguments...");
        log.debug("Arguments: {}", Arrays.toString(args));

        exitCode = ExitCode.INVALID_ARGS;

        try {
            argumentParser.parse(args);
        } catch (ParameterException e) {
            throw new IllegalArgumentException("Failed to parse arguments: " + e.getMessage(), e);
        }

        final String commandName = argumentParser.getSelectedCommand();
        final Set<String> availableCommands = argumentParser.getCommandNames();
        final String availableCommandsString = Arrays.toString(availableCommands.toArray());

        if (commandName == null) {
            throw new IllegalArgumentException("No command selected -- commands available: " + availableCommandsString);
        } else if (!availableCommands.contains(commandName)) {
            throw new IllegalArgumentException("Invalid command -- commands available: " + availableCommandsString);
        }

        UserConsole.getLogger().info("Initializing command {}", commandName);

        log.info("Initializing command: {}", commandName);
        exitCode = ExitCode.COMMAND_INIT_FAIL;
        final AgentCommand command = commandFactory.get(commandName);

        exitCode = ExitCode.EXEC_FAIL;
        command.run();
        exitCode = ExitCode.SUCCESS;
    }

    @Override
    public int getExitCode() {
        UserConsole.getLogger().info("Terminating with code: {} ({})", exitCode.getCode(), exitCode.getMessage());
        return exitCode.getCode();
    }
}
