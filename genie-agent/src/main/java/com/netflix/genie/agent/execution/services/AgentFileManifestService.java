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

package com.netflix.genie.agent.execution.services;

import java.nio.file.Path;

/**
 * This service regularly produces a manifest of the executing job folder and pushes it to the server.
 *
 * @author mprimi
 * @since 4.0.0
 */
public interface AgentFileManifestService {
    /**
     * Start the service.
     *
     * @param claimedJobId     the claimed job id
     * @param jobDirectoryPath the job directory
     */
    void start(String claimedJobId, Path jobDirectoryPath);

    /**
     * Stop the service.
     */
    void stop();
}
