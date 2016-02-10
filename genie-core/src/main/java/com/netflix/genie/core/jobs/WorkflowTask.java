package com.netflix.genie.core.jobs;

import com.netflix.genie.common.exceptions.GenieException;

/**
 * Interface that defines a task in a workflow.
 *
 * @author amsharma
 */
public interface WorkflowTask {

    /**
     * Execute the task.
     *
     * @param context Information needed to execute the task
     * @throws GenieException if there is an error
     */
    void executeTask(
        Context context
    ) throws GenieException;
}
