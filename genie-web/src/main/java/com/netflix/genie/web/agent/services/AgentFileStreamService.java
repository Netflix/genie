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
package com.netflix.genie.web.agent.services;

import com.netflix.genie.common.internal.dtos.DirectoryManifest;
import org.springframework.core.io.Resource;

import javax.validation.constraints.NotBlank;
import java.net.URI;
import java.nio.file.Path;
import java.util.Optional;

/**
 * Service to retrieve files from a remote agent while the latter is executing a job.
 * The file is returned as a {@link Resource} so it can be, for example, returned by the server via web API.
 *
 * @author mprimi
 * @since 4.0.0
 */
public interface AgentFileStreamService {

    /**
     * Returns a Resource for the given job file boxed in an {@link Optional}.
     * If the service is unable to determine whether the file exists, the optional is empty.
     * In all other cases, the optional is not empty. However the resource may return false to {@code exist()} calls
     * (if the file is not believed to exist on the agent) or false to {@code isReadable()} if the file cannot be
     * streamed for other reasons.
     *
     * @param jobId        the job id
     * @param relativePath the relative path in the job directory
     * @param uri          the file uri //TODO redundant
     * @return an optional {@link Resource}
     */
    Optional<AgentFileResource> getResource(@NotBlank String jobId, Path relativePath, URI uri);

    /**
     * Returns the manifest for a given job, boxed in an {@link Optional}.
     * The manifest may not be present if the agent is not connected to this node (for example because execution has
     * completed, or because the agent is connected to a different node).
     *
     * @param jobId the job id
     * @return an optional {@link DirectoryManifest}
     */
    Optional<DirectoryManifest> getManifest(String jobId);

    /**
     * A {@link Resource} for files local to a remote agent.
     *
     * @author mprimi
     * @since 4.0.0
     */
    interface AgentFileResource extends Resource {
    }
}
