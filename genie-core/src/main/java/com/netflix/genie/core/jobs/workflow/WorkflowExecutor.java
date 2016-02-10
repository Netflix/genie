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
package com.netflix.genie.core.jobs.workflow;

import java.util.List;

/**
 * Execute a workflow. Any class implementing this interface should be able to take in a list of impl and execute them
 * in order.
 *
 * @author amsharma
 * @since 3.0.0
 */
public interface WorkflowExecutor {

    /**
     * Execute the workflow using the list of impl provided.
     *
     * @param tasks List of workflow impl to be executed
     * @param context Information needed by individual impl in the workflow
     *
     * @return return true in case of successfull execution of workflow.
     */
    boolean executeWorkflow(List<WorkflowTask> tasks, Context context);
}
