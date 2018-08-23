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
import org.springframework.stereotype.Component;

/**
 * Implementation of AgentOptions delegate.
 */
@Component
public class AgentOptionsArgumentsImpl implements ArgumentDelegates.AgentOptions {

    @Parameter(
        names = {"--no-cleanup"},
        description = "Don't remove the job folder"
    )
    private boolean noCleanup;

    @Parameter(
        names = {"--full-cleanup"},
        description = "Remove the job folder"
    )
    private boolean fullCleanup;

    @Override
    public JobFolderCleanupOption getJobFolderCleanUpOption() {
        if (noCleanup) {
            return JobFolderCleanupOption.NO_CLEANUP;
        } else if (fullCleanup) {
            return JobFolderCleanupOption.DELETE_JOB_FOLDER;
        } else {
            return JobFolderCleanupOption.DELETE_DEPENDENCIES_ONLY;
        }
    }
}
