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

import com.netflix.genie.agent.execution.CleanupStrategy;
import com.netflix.genie.agent.execution.exceptions.SetUpJobException;
import com.netflix.genie.common.internal.dto.v4.JobSpecification;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * Service that sets up a directory for a job to execute in.
 * The setup is broken into 3 stages so that they can be interleaved with other actions.
 * 1. Create the empty folder structure
 * 2. Download dependencies and other files in place
 * 3. Evaluate setup scripts and create the job environment.
 * This service also performs cleanup of said job directory.
 *
 * @author mprimi
 * @since 4.0.0
 */
public interface JobSetupService {

    /**
     * Creates a directory for the given job.
     * Also creates the sub-directories structure common to all jobs.
     *
     * @param jobSpecification the job specification
     * @return the job folder just created
     * @throws SetUpJobException if the folder (or sub-folders) could not be created or already existed
     */
    File createJobDirectory(
        JobSpecification jobSpecification
    ) throws SetUpJobException;

    /**
     * Downloads and stages all the job files (dependencies, configurations, ...) into the job directory.
     *
     * @param jobSpecification the job specification
     * @param jobDirectory     the job folder
     * @return the list of setup files staged
     * @throws SetUpJobException TODO
     */
    List<File> downloadJobResources(
        JobSpecification jobSpecification,
        File jobDirectory
    ) throws SetUpJobException;

    /**
     * Execute setup scripts for various entities which may alter the layout of the job directory (example: expand
     * archive dependencies) and/or alter the environment.
     *
     * @param jobDirectory     the job directory
     * @param jobSpecification the job specification
     * @param setupFiles       the list of setup files to evaluate
     * @return a snapshot of environment variables and their values after setup is complete
     * @throws SetUpJobException TODO
     */
    Map<String, String> setupJobEnvironment(
        File jobDirectory,
        JobSpecification jobSpecification,
        List<File> setupFiles
    ) throws SetUpJobException;

    /**
     * Performs post-execution cleanup of the job directory.
     *
     * @param jobDirectory    the job directory path
     * @param cleanupStrategy the cleanup strategy
     * @throws IOException TODO
     */
    void cleanupJobDirectory(
        Path jobDirectory,
        CleanupStrategy cleanupStrategy
    ) throws IOException;
}

