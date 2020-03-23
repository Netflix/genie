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

import com.netflix.genie.agent.execution.statemachine.ExecutionContext;
import com.netflix.genie.agent.execution.statemachine.ExecutionStage;
import com.netflix.genie.agent.execution.statemachine.FatalTransitionException;
import com.netflix.genie.agent.execution.statemachine.RetryableTransitionException;
import com.netflix.genie.agent.execution.statemachine.States;
import com.netflix.genie.common.external.dtos.v4.JobSpecification;
import com.netflix.genie.common.internal.exceptions.checked.JobArchiveException;
import com.netflix.genie.common.internal.services.JobArchiveService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Archive job output files and logs, if the job reached a state where it is appropriate to do so.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
public class ArchiveJobOutputsStage extends ExecutionStage {
    private final JobArchiveService jobArchiveService;

    /**
     * Constructor.
     *
     * @param jobArchiveService job archive service
     */
    public ArchiveJobOutputsStage(final JobArchiveService jobArchiveService) {
        super(States.ARCHIVE);
        this.jobArchiveService = jobArchiveService;
    }

    @Override
    protected void attemptTransition(
        final ExecutionContext executionContext
    ) throws RetryableTransitionException, FatalTransitionException {

        final JobSpecification jobSpecification = executionContext.getJobSpecification();
        final File jobDirectory = executionContext.getJobDirectory();

        if (jobSpecification != null && jobDirectory != null) {
            final String archiveLocation = jobSpecification.getArchiveLocation().orElse(null);
            if (StringUtils.isNotBlank(archiveLocation)) {
                try {
                    log.info("Archive job folder to: " + archiveLocation);
                    this.jobArchiveService.archiveDirectory(
                        jobDirectory.toPath(),
                        new URI(archiveLocation)
                    );
                } catch (JobArchiveException | URISyntaxException e) {
                    // Swallow the error and move on.
                    log.error("Error archiving job folder", e);
                }
            }
        }
    }
}

