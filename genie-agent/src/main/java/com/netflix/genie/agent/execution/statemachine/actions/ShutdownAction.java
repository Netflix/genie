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

package com.netflix.genie.agent.execution.statemachine.actions;

import com.netflix.genie.agent.execution.ExecutionContext;
import com.netflix.genie.agent.execution.exceptions.ArchivalException;
import com.netflix.genie.agent.execution.services.ArchivalService;
import com.netflix.genie.agent.execution.statemachine.Events;
import com.netflix.genie.common.internal.dto.v4.JobSpecification;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * Action performed when in state SHUTDOWN.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
class ShutdownAction extends BaseStateAction implements StateAction.Shutdown {

    private final ArchivalService archivalService;

    ShutdownAction(
        final ExecutionContext executionContext,
        final ArchivalService archivalService
    ) {
        super(executionContext);
        this.archivalService = archivalService;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Events executeStateAction(final ExecutionContext executionContext) {
        log.info("Shutting down...");

        try {
            final JobSpecification jobSpecification = executionContext.getJobSpecification();
            final String archiveLocation = jobSpecification.getArchiveLocation().orElse(null);
            if (StringUtils.isNotBlank(archiveLocation)) {
                log.info("Attempting to archive job folder to: " + archiveLocation);
                archivalService.archive(
                    executionContext.getJobDirectory().toPath(),
                    new URI(archiveLocation)
                );
            } else {
                log.info("Job folder archival location is empty. Skipping archiving the job folder.");
            }
            log.info("Job folder archived to: " + archiveLocation);
        } catch (ArchivalException | URISyntaxException e) {
            log.error("Error archiving job folder", e);
        }

        return Events.SHUTDOWN_COMPLETE;
    }
}
