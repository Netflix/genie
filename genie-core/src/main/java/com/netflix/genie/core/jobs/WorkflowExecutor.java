package com.netflix.genie.core.jobs;

import java.util.List;

/**
 * Execute a workflow. Any class implementing this interface should be able to take in a list of tasks and execute them
 * in order.
 *
 * @author amsharma
 */
public interface WorkflowExecutor {

    /**
     * Execute the workflow using the list of tasks provided.
     *
     * @param tasks List of workflow tasks to be executed
     * @param context Information needed by individual tasks in the workflow
     *
     * @return return true in case of successfull execution of workflow.
     */
    boolean executeWorkflow(List<WorkflowTask> tasks, Context context);
}
