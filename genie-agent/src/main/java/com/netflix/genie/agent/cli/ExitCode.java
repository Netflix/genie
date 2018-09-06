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

/**
 * Exit codes for Genie agent.
 *
 * @author mprimi
 * @since 4.0.0
 */
public enum ExitCode {

    /**
     * Agent command execution completed successfully.
     */
    SUCCESS(0, "Success"),

    /**
     * Agent failed to bootstrap and/or initialize.
     */
    INIT_FAIL(101, "Agent initialization error"),

    /**
     * Command-line options provided are invalid.
     */
    INVALID_ARGS(102, "Invalid arguments"),

    /**
     * The selected agent command failed to initialize.
     */
    COMMAND_INIT_FAIL(103, "Command initialization error"),

    /**
     * The selected agent command failed to execute.
     */
    EXEC_FAIL(104, "Command execution error"),

    /**
     * The selected agent command was forcefully terminated before completion.
     */
    EXEC_ABORTED(105, "Command execution aborted");

    static final String EXIT_CODE_HELP_MESSAGE;

    static {
        final StringBuilder stringBuilder = new StringBuilder();
        stringBuilder
            .append("EXIT CODES:").append("\n");

        for (final ExitCode exitCode : ExitCode.values()) {
            stringBuilder
                .append(exitCode.getCode())
                .append(": ")
                .append(exitCode.name())
                .append(" - ")
                .append(exitCode.getMessage())
                .append("\n");
        }

        EXIT_CODE_HELP_MESSAGE = stringBuilder.toString();
    }

    private final int code;
    private final String message;

    ExitCode(final int code, final String message) {
        this.code = code;
        this.message = message;
    }

    int getCode() {
        return code;
    }

    String getMessage() {
        return message;
    }
}
