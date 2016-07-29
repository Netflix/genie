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
package com.netflix.genie.core.jobs.workflow.impl;

import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.core.jobs.JobConstants;
import com.netflix.genie.core.jobs.JobExecutionEnvironment;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Timer;
import lombok.extern.slf4j.Slf4j;

import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.io.Writer;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * This class is responsible for adding the kill handling logic to run.sh.
 *
 * @author amsharma
 * @since 3.0.0
 */
@Slf4j
public class JobFailureAndKillHandlerLogicTask extends GenieBaseTask {

    private final Timer timer;

    /**
     * Constructor.
     *
     * @param registry The metrics registry to use
     */
    public JobFailureAndKillHandlerLogicTask(@NotNull final Registry registry) {
        this.timer = registry.timer("genie.jobs.tasks.jobFailureAndKillHandlerLogicTask.timer");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void executeTask(@NotNull final Map<String, Object> context) throws GenieException, IOException {
        final long start = System.nanoTime();
        try {
            final JobExecutionEnvironment jobExecEnv
                = (JobExecutionEnvironment) context.get(JobConstants.JOB_EXECUTION_ENV_KEY);
            log.info("Starting Job Failure and Kill Handler Task for job {}", jobExecEnv.getJobRequest().getId());

            final Writer writer = (Writer) context.get(JobConstants.WRITER_KEY);

            // Append logic for handling job kill signal
            writer.write(JobConstants.JOB_FAILURE_AND_KILL_HANDLER_LOGIC + System.lineSeparator());
            log.info("Finished Job Failure and Kill Handler Task for job {}", jobExecEnv.getJobRequest().getId());
        } finally {
            final long finish = System.nanoTime();
            this.timer.record(finish - start, TimeUnit.NANOSECONDS);
        }
    }
}
