/*
 *
 *  Copyright 2016 Netflix, Inc.
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
package com.netflix.genie.web.util;

import com.netflix.genie.common.exceptions.GenieTimeoutException;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.Executor;
import org.apache.commons.lang3.SystemUtils;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.time.Instant;

/**
 * Implementation of ProcessChecker for Unix based systems.
 *
 * @author tgianos
 * @since 3.0.0
 */
public class UnixProcessChecker implements ProcessChecker {

    private final Executor executor;
    private final CommandLine commandLine;
    private final Instant timeout;

    /**
     * Constructor.
     *
     * @param pid           The process id to check.
     * @param executor      The executor to use for generating system commands.
     * @param timeout       The time which after this job should be killed due to timeout
     * @param checkWithSudo Whether the checker requires sudo
     */
    UnixProcessChecker(
        @Min(1) final int pid,
        @NotNull final Executor executor,
        @NotNull final Instant timeout,
        final boolean checkWithSudo
    ) {
        if (!SystemUtils.IS_OS_UNIX) {
            throw new IllegalArgumentException("Not running on a Unix system.");
        }

        this.executor = executor;

        // Use POSIX compliant 'kill -0 <PID>' to check if process is still running.
        if (checkWithSudo) {
            this.commandLine = new CommandLine("sudo");
            this.commandLine.addArgument("kill");
            this.commandLine.addArgument("-0");
            this.commandLine.addArgument(Integer.toString(pid));
        } else {
            this.commandLine = new CommandLine("kill");
            this.commandLine.addArgument("-0");
            this.commandLine.addArgument(Integer.toString(pid));
        }

        this.timeout = timeout;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void checkProcess() throws GenieTimeoutException, ExecuteException, IOException {
        this.executor.execute(this.commandLine);

        // If we get here the process is still running. Check if it should be killed due to timeout.
        if (Instant.now().isAfter(this.timeout)) {
            throw new GenieTimeoutException(
                "Job has exceeded its timeout time of " + this.timeout
            );
        }
    }

    /**
     * Factory for {@link ProcessChecker} for UNIX systems.
     */
    public static class Factory implements ProcessChecker.Factory {

        private final Executor executor;
        private final boolean checkWithSudo;

        /**
         * Constructor.
         *
         * @param executor      The executor used by process checkers
         * @param checkWithSudo Whether the check requires sudo
         */
        public Factory(final Executor executor, final boolean checkWithSudo) {
            this.executor = executor;
            this.checkWithSudo = checkWithSudo;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public ProcessChecker get(final int pid, final Instant timeout) {
            return new UnixProcessChecker(pid, this.executor, timeout, this.checkWithSudo);
        }
    }
}
