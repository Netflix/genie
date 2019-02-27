/*
 *
 *  Copyright 2019 Netflix, Inc.
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

package com.netflix.genie.web.services.impl;

import com.google.common.collect.Maps;
import com.netflix.genie.common.internal.dto.JobDirectoryManifest;
import com.netflix.genie.web.services.AgentFileManifestService;
import lombok.extern.slf4j.Slf4j;

import javax.validation.constraints.NotBlank;
import java.util.Map;
import java.util.Optional;

/**
 * Implementation of {@link AgentFileManifestService} using gRPC.
 * Tracks the most recently received copy of the file manifest for each agent connected until it is explicitly removed.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
public class AgentFileManifestServiceImpl implements AgentFileManifestService {
    private final Map<String, JobDirectoryManifest> manifestMap = Maps.newConcurrentMap();

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateManifest(@NotBlank final String jobId, final JobDirectoryManifest manifest) {
        final JobDirectoryManifest previousManifest = this.manifestMap.put(jobId, manifest);

        if (previousManifest == null) {
            log.debug("Added manifest for new job: {}", jobId);
        } else if (previousManifest.equals(manifest)) {
            log.debug("Latest manifest is no different from existing one for job: {}", jobId);
        } else {
            log.debug("Updated manifest for for job: {}", jobId);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<JobDirectoryManifest> getManifest(@NotBlank final String jobId) {
        return Optional.ofNullable(manifestMap.get(jobId));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteManifest(@NotBlank final String jobId) {
        this.manifestMap.remove(jobId);
    }
}
