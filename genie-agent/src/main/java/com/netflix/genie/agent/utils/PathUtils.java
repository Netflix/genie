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

package com.netflix.genie.agent.utils;

import com.netflix.genie.common.internal.jobs.JobConstants;

import java.io.File;
import java.nio.file.Path;

/**
 * Utilities to compose filesystem paths.
 *
 * @author mprimi
 * @since 4.0.0
 */
public final class PathUtils {

    private PathUtils() {
    }

    /**
     * Append an arbitrary set of components to a base path.
     *
     * @param baseDirectory the base directory
     * @param children      path components
     * @return a Path
     */
    public static Path composePath(
        final File baseDirectory,
        final String... children
    ) {
        File pathAccumulator = baseDirectory;
        for (String child : children) {
            pathAccumulator = new File(pathAccumulator, child);
        }
        return pathAccumulator.toPath();
    }

    /**
     * Append an arbitrary set of components to a base path.
     *
     * @param baseDirectory the base directory
     * @param children      path components
     * @return a Path
     */
    public static Path composePath(
        final Path baseDirectory,
        final String... children
    ) {
        return composePath(baseDirectory.toFile(), children);
    }


    /**
     * Compose the path to the applications directory inside a job directory.
     *
     * @param jobDirectory the job directory
     * @return a Path
     */
    public static Path jobApplicationsDirectoryPath(
        final File jobDirectory
    ) {
        return composePath(
            jobDirectory,
            JobConstants.GENIE_PATH_VAR,
            JobConstants.APPLICATION_PATH_VAR
        );
    }

    /**
     * Compose the path to an application directory inside a job directory.
     *
     * @param jobDirectory the job directory
     * @param appId        the application id
     * @return a Path
     */
    public static Path jobApplicationDirectoryPath(
        final File jobDirectory,
        final String appId
    ) {
        return composePath(
            jobApplicationsDirectoryPath(jobDirectory),
            appId
        );
    }

    /**
     * Compose the path to the cluster directory inside a job directory.
     *
     * @param jobDirectory the job directory
     * @param clusterId    the cluster id
     * @return a Path
     */
    public static Path jobClusterDirectoryPath(final File jobDirectory, final String clusterId) {
        return composePath(
            jobDirectory,
            JobConstants.GENIE_PATH_VAR,
            JobConstants.CLUSTER_PATH_VAR,
            clusterId
        );
    }

    /**
     * Compose the path to the command directory inside a job directory.
     *
     * @param jobDirectory the job directory
     * @param commandId    the command id
     * @return a Path
     */
    public static Path jobCommandDirectoryPath(final File jobDirectory, final String commandId) {
        return composePath(
            jobDirectory,
            JobConstants.GENIE_PATH_VAR,
            JobConstants.COMMAND_PATH_VAR,
            commandId
        );
    }

    /**
     * Compose the path to the genie directory inside a job directory.
     *
     * @param jobDirectory the job directory
     * @return a Path
     */
    public static Path jobGenieDirectoryPath(final File jobDirectory) {
        return composePath(
            jobDirectory,
            JobConstants.GENIE_PATH_VAR
        );
    }

    /**
     * Compose the path to the genie logs directory inside a job directory.
     *
     * @param jobDirectory the job directory
     * @return a Path
     */
    public static Path jobGenieLogsDirectoryPath(final File jobDirectory) {
        return composePath(
            jobDirectory,
            JobConstants.GENIE_PATH_VAR,
            JobConstants.LOGS_PATH_VAR
        );
    }

    /**
     * Compose the path to the dependencies directory for a given entity.
     *
     * @param entityDirectory the entity base directory
     * @return a Path
     */
    public static Path jobEntityDependenciesPath(final Path entityDirectory) {
        return composePath(
            new File(entityDirectory.toUri()),
            JobConstants.DEPENDENCY_FILE_PATH_PREFIX
        );
    }

    /**
     * Compose the path to the configurations directory for a given entity.
     *
     * @param entityDirectory the entity base directory
     * @return a Path
     */
    public static Path jobEntityConfigPath(final Path entityDirectory) {
        return composePath(
            new File(entityDirectory.toUri()),
            JobConstants.CONFIG_FILE_PATH_PREFIX
        );
    }

    /**
     * Compose the path to the standard output log file for a job.
     *
     * @param jobDirectory the job directory
     * @return a Path
     */
    public static Path jobStdOutPath(final File jobDirectory) {
        return composePath(
            jobDirectory,
            JobConstants.STDOUT_LOG_FILE_NAME
        );
    }

    /**
     * Compose the path to the standard error log file for a job.
     *
     * @param jobDirectory the job directory
     * @return a Path
     */
    public static Path jobStdErrPath(final File jobDirectory) {
        return composePath(
            jobDirectory,
            JobConstants.STDERR_LOG_FILE_NAME
        );
    }
}
