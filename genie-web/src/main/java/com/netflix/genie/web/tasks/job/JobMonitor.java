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
package com.netflix.genie.web.tasks.job;

import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.core.events.JobFinishedEvent;
import com.netflix.genie.web.tasks.GenieTaskScheduleType;
import com.netflix.genie.web.tasks.node.NodeTask;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.Executor;
import org.apache.commons.lang3.SystemUtils;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.scheduling.Trigger;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Given a process id this class will check if the job client process is running or not.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Slf4j
public class JobMonitor implements NodeTask {

    private static final String PID_KEY = "pid";
    private static final int PS_SUCCESS = 0;

    private final JobExecution execution;
    private final Executor executor;
    private final ApplicationEventPublisher publisher;

    /**
     * Constructor.
     *
     * @param execution The job execution object including the pid
     * @param executor  The process executor to use
     * @param publisher The event publisher to use when a job isn't running anymore
     */
    public JobMonitor(
        @Valid final JobExecution execution,
        @NotNull final Executor executor,
        @NotNull final ApplicationEventPublisher publisher
    ) {
        this.execution = execution;
        this.executor = executor;
        this.publisher = publisher;
    }

    /**
     * This will check the process identified by the pid supplied to the constructor. If the pid no longer exists fires
     * an event to the system saying the job is done.
     */
    @Override
    public void run() {
        if (!SystemUtils.IS_OS_UNIX) {
            throw new UnsupportedOperationException("Genie doesn't currently support " + SystemUtils.OS_NAME);
        }

        final int pid = this.execution.getProcessId();
        final Map<String, Object> substitutionMap = new HashMap<>();
        substitutionMap.put(PID_KEY, pid);

        final CommandLine commandLine = new CommandLine("ps");
        commandLine.addArgument("-p");
        commandLine.addArgument("${" + PID_KEY + "}");
        commandLine.setSubstitutionMap(substitutionMap);

        // TODO: Should we add a timeout?
        try {
            // Blocks until result
            final int result = this.executor.execute(commandLine);
            if (result == PS_SUCCESS) {
                //TODO: If the process is still running check the length of the Stdout and Stderr
                log.debug("Job {} is still running...", this.execution.getId());
            } else {
                log.info("Job {} has finished", this.execution.getId());
                this.publisher.publishEvent(new JobFinishedEvent(this.execution, this));
            }
        } catch (final IOException ioe) {
            // Some other error
            log.error("Some IOException happened unable to check process status for pid {}", pid, ioe);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GenieTaskScheduleType getScheduleType() {
        return GenieTaskScheduleType.FIXED_DELAY;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Trigger getTrigger() {
        throw new UnsupportedOperationException("Tracking can only currently be scheduled via fixed delay");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getFixedRate() {
        throw new UnsupportedOperationException("Tracking can only currently be scheduled via fixed delay");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getFixedDelay() {
        //TODO: Make flexible via property
        return 1000L;
    }
}
