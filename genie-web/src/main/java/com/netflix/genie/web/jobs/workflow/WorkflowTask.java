/*
 *
 *  Copyright 2015 Netflix, Inc.
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
package com.netflix.genie.web.jobs.workflow;

import com.netflix.genie.common.exceptions.GenieException;

import java.io.IOException;
import java.util.Map;

/**
 * Interface that defines a task in a workflow.
 *
 * @author amsharma
 * @since 3.0.0
 */
public interface WorkflowTask {

    /**
     * Execute the task.
     *
     * @param context Information needed to execute the task.
     * @throws GenieException if there is an error.
     * @throws IOException    if there is a problem writing to the disk.
     */
    void executeTask(
        Map<String, Object> context
    ) throws GenieException, IOException;
}
