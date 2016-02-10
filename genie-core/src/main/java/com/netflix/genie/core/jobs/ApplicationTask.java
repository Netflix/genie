package com.netflix.genie.core.jobs;

import com.netflix.genie.common.exceptions.GenieException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import javax.validation.constraints.NotNull;

/**
 * Implementation of the workflow task for handling Applications that a job needs.
 *
 * @author amsharma
 */
@Slf4j
@Component
public class ApplicationTask implements WorkflowTask {

    /**
     * {@inheritDoc}
     */
    @Override
    public void executeTask(
        @NotNull
        final Context context
    ) throws GenieException {
        log.info("Execution Application Task in the workflow.");
    }
}
