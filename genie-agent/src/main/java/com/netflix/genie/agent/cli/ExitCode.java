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
enum ExitCode {
    SUCCESS(0, "Success"),
    INIT_FAIL(101, "Agent initialization error"),
    INVALID_ARGS(102, "Invalid arguments"),
    COMMAND_INIT_FAIL(103, "Command initialization error"),
    EXEC_FAIL(104, "Command execution error");

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
