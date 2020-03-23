/*
 *
 *  Copyright 2020 Netflix, Inc.
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
package com.netflix.genie.agent.execution.statemachine.stages;

import com.netflix.genie.agent.cli.UserConsole;
import com.netflix.genie.agent.utils.PathUtils;
import com.netflix.genie.agent.execution.statemachine.ExecutionStage;
import com.netflix.genie.agent.execution.statemachine.FatalTransitionException;
import com.netflix.genie.agent.execution.statemachine.States;
import com.netflix.genie.agent.execution.statemachine.RetryableTransitionException;
import com.netflix.genie.agent.execution.statemachine.ExecutionContext;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

/**
 * Moves the agent log file into the job directory from the temporary location.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
public class RelocateLogFileStage extends ExecutionStage {
    /**
     * Constructor.
     */
    public RelocateLogFileStage() {
        super(States.RELOCATE_LOG);
    }

    @Override
    protected void attemptTransition(
        final ExecutionContext executionContext
    ) throws RetryableTransitionException, FatalTransitionException {

        final File jobDirectory = executionContext.getJobDirectory();
        assert jobDirectory != null;

        final Path destinationPath = PathUtils.jobAgentLogFilePath(jobDirectory);

        log.info("Relocating agent log file to: {}", destinationPath);

        try {
            UserConsole.relocateLogFile(destinationPath);
        } catch (IOException e) {
            log.error("Failed to relocate agent log file", e);
        }
    }
}
