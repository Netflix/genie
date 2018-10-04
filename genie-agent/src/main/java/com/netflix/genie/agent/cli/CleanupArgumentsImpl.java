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

package com.netflix.genie.agent.cli;

import com.beust.jcommander.Parameter;
import com.netflix.genie.agent.execution.CleanupStrategy;

/**
 * Implementation of {@link ArgumentDelegates.CleanupArguments} delegate.
 *
 * @author mprimi
 * @since 4.0.0
 */
class CleanupArgumentsImpl implements ArgumentDelegates.CleanupArguments {

    @Parameter(names = {"--no-cleanup"}, description = "Skip the post-execution cleanup and leave all files in place")
    private boolean skipCleanup;

    @Parameter(names = {"--full-cleanup"}, description = "Remove the entire job folder post-execution")
    private boolean fullCleanup;

    /**
     * {@inheritDoc}
     */
    @Override
    public CleanupStrategy getCleanupStrategy() {
        if (skipCleanup) {
            return CleanupStrategy.NO_CLEANUP;
        } else if (fullCleanup) {
            return CleanupStrategy.FULL_CLEANUP;
        }

        return CleanupStrategy.DEPENDENCIES_CLEANUP;
    }
}
