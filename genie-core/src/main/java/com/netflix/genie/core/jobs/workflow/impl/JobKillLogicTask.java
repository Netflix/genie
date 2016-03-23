package com.netflix.genie.core.jobs.workflow.impl;

import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.core.jobs.JobConstants;
import com.netflix.genie.core.util.Utils;
import lombok.extern.slf4j.Slf4j;

import javax.validation.constraints.NotNull;
import java.util.Map;

/**
 * This class is responsible for adding the kill handling logic to run.sh.
 *
 * @author amsharma
 */
@Slf4j
public class JobKillLogicTask extends GenieBaseTask {
    /**
     * {@inheritDoc}
     */
    @Override
    public void executeTask(
        @NotNull
        final Map<String, Object> context
    ) throws GenieException {

        log.debug("Executing JobKillLogic Task in the workflow.");
        super.executeTask(context);

        // Append logic for handling job kill signal
        Utils.appendToWriter(writer, JobConstants.JOB_KILL_HANDLER_LOGIC.toString());
    }
}
