package com.netflix.genie.core.jobs;

import com.netflix.genie.common.exceptions.GenieException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import javax.validation.constraints.NotNull;
import java.util.List;

/**
 * Implements the workflow executor to execute the job workflow.
 *
 * @author amsharma
 */
@Slf4j
@Component
public class JobWorkflowExecutor implements WorkflowExecutor {

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean executeWorkflow(
        @NotNull
        final List<WorkflowTask> tasks,
        @NotNull
        final Context context
    ) {

        try {
            for (WorkflowTask task : tasks) {
                task.executeTask(context);
            }
        } catch (GenieException ge) {
            log.error("Failed to execute task with exception {}", ge);
            return false;
        }

        // TODO need to throw this exception
//        tasks.forEach(workflowTask -> {
//            try {
//                workflowTask.executeTask(context);
//            } catch (GenieException e) {
//                log.error("Got Exception {} while executing task", e.toString());
//            }
//        });
        return true;
    }
}
