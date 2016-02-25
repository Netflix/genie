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
