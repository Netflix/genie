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
package com.netflix.genie.agent.execution.services;

import java.nio.file.Path;

/**
 * Service that monitors the job directory and may decide to kill the job if some limit is exceeded.
 * For example, if a file grows larger than some amount.
 *
 * @author mprimi
 * @since 4.0.0
 */
public interface JobMonitorService {

    /**
     * Starts the service.
     *
     * @param jobId        the job id
     * @param jobDirectory the job directory
     */
    void start(String jobId, Path jobDirectory);

    /**
     * Stop the service.
     */
    void stop();
}
