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

package com.netflix.genie.web.services;

import com.netflix.genie.common.internal.dto.JobDirectoryManifest;
import org.springframework.validation.annotation.Validated;

import javax.annotation.concurrent.ThreadSafe;
import javax.validation.constraints.NotBlank;
import java.util.Optional;

/**
 * Service that caches in-memory {@link JobDirectoryManifest} of agents currently connected to this node.
 * Manifests are then used to serve directory listing requests for agent jobs running.
 *
 * @author mprimi
 * @since 4.0.0
 */
@ThreadSafe
@Validated
public interface AgentFileManifestService {

    /**
     * Store a {@link JobDirectoryManifest} for a given job.
     *
     * @param jobId    the job id
     * @param manifest the manifest
     */
    void updateManifest(@NotBlank String jobId, JobDirectoryManifest manifest);

    /**
     * Retrieve the {@link JobDirectoryManifest} for a given job.
     *
     * @param jobId the job id
     * @return an optional which may contain the manifest
     */
    Optional<JobDirectoryManifest> getManifest(@NotBlank String jobId);

    /**
     * Remove the {@link JobDirectoryManifest} for a given job, if one is stored.
     *
     * @param jobId    the job id
     */
    void deleteManifest(@NotBlank String jobId);
}
