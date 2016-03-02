/*
 *
 *  Copyright 2016 Netflix, Inc.
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
package com.netflix.genie.core.jobs.workflow.impl;

import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.core.jobs.workflow.WorkflowExecutor;
import com.netflix.genie.core.jobs.workflow.WorkflowTask;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;

/**
 * Implements the workflow executor to execute the job workflow.
 *
 * @author amsharma
 * @since 3.0.0
 */
@Slf4j
@Component
public class SequenceWorkflowExecutor implements WorkflowExecutor {

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean executeWorkflow(
        @NotNull
        final List<WorkflowTask> tasks,
        @NotNull
        final Map<String, Object> context
    ) {
        try {
            for (WorkflowTask task : tasks) {
                task.executeTask(context);
            }
        } catch (GenieException ge) {
            log.error("Failed to execute task with exception {}", ge);
            return false;
        }
        return true;
    }
}
