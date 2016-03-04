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
package com.netflix.genie.core.util;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.Executor;
import org.apache.commons.lang3.SystemUtils;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of ProcessChecker for Unix based systems.
 *
 * @author tgianos
 * @since 3.0.0
 */
public class UnixProcessChecker implements ProcessChecker {

    protected static final String PID_KEY = "pid";

    private final Executor executor;
    private final CommandLine commandLine;

    /**
     * Constructor.
     *
     * @param pid      The process id to check.
     * @param executor The executor to use for generating system commands.
     */
    public UnixProcessChecker(@Min(1) final int pid, @NotNull final Executor executor) {
        if (!SystemUtils.IS_OS_UNIX) {
            throw new IllegalArgumentException("Not running on a Unix system.");
        }

        this.executor = executor;

        final Map<String, Object> substitutionMap = new HashMap<>();
        substitutionMap.put(PID_KEY, pid);

        // Using PS for now but could instead check for existence of done file if this proves to have bad performance
        // send output to /dev/null so it doesn't print to the logs
        this.commandLine = new CommandLine("ps");
        this.commandLine.addArgument("-p");
        this.commandLine.addArgument("${" + PID_KEY + "}");
        this.commandLine.setSubstitutionMap(substitutionMap);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void checkProcess() throws ExecuteException, IOException {
        this.executor.execute(this.commandLine);
    }
}
