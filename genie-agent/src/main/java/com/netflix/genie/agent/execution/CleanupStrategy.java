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
package com.netflix.genie.agent.execution;

/**
 * Enum to describe the different kind of post-execution cleanup of the job directory.
 *
 * @author mprimi
 * @since 4.0.0
 */
public enum CleanupStrategy {

    /**
     * Skip cleanup completely.
     */
    NO_CLEANUP,

    /**
     * Wipe the entire job directory.
     */
    FULL_CLEANUP,

    /**
     * Clean the contents of dependencies sub-directories, leave the rest.
     */
    DEPENDENCIES_CLEANUP;

    /**
     * Section of the help message explaining the cleanup strategy.
     */
    public static final String CLEANUP_HELP_MESSAGE = "JOB DIRECTORY CLEANUP:\n"
        + "The default cleanup behavior is to delete downloaded dependencies after job execution completed\n"
        + "(whether or not it was successful). A different strategy (no cleanup, full cleanup, ...) can be\n"
        + "selected via command-line flags";
}
