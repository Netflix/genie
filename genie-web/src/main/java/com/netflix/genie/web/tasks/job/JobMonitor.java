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
import com.netflix.genie.common.exceptions.GenieTimeoutException;
import com.netflix.genie.core.events.JobFinishedEvent;
import com.netflix.genie.core.events.KillJobEvent;
import com.netflix.genie.core.util.ProcessChecker;
import com.netflix.genie.core.util.UnixProcessChecker;
import com.netflix.genie.web.tasks.GenieTaskScheduleType;
import com.netflix.genie.web.tasks.node.NodeTask;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.Executor;
import org.apache.commons.lang3.SystemUtils;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.scheduling.Trigger;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.io.IOException;

/**
 * Given a process id this class will check if the job client process is running or not.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Slf4j
public class JobMonitor implements NodeTask {

    // How many error iterations we can handle
    // TODO: Make this a variable
    private static final int MAX_ERRORS = 5;
    private final JobExecution execution;
    private final ProcessChecker processChecker;
    private final ApplicationEventPublisher publisher;
    private int errorCount;

    // Metrics
    private final Counter successfulCheckRate;
    private final Counter timeoutRate;
    private final Counter finishedRate;
    private final Counter unsuccessfulCheckRate;

    /**
     * Constructor.
     *
     * @param execution The job execution object including the pid
     * @param executor  The process executor to use
     * @param publisher The event publisher to use when a job isn't running anymore
     * @param registry  The metrics event registry
     */
    public JobMonitor(
        @Valid final JobExecution execution,
        @NotNull final Executor executor,
        @NotNull final ApplicationEventPublisher publisher,
        @NotNull final Registry registry
    ) {
        if (!SystemUtils.IS_OS_UNIX) {
            throw new UnsupportedOperationException("Genie doesn't currently support " + SystemUtils.OS_NAME);
        }

        this.errorCount = 0;
        this.execution = execution;
        this.publisher = publisher;
        this.processChecker = new UnixProcessChecker(execution.getProcessId(), executor, execution.getTimeout());
        this.successfulCheckRate = registry.counter("genie.jobs.successfulStatusCheck.rate");
        this.timeoutRate = registry.counter("genie.jobs.timeout.rate");
        this.finishedRate = registry.counter("genie.jobs.finished.rate");
        this.unsuccessfulCheckRate = registry.counter("genie.jobs.unsuccessfulStatusCheck.rate");
    }

    /**
     * This will check the process identified by the pid supplied to the constructor. If the pid no longer exists fires
     * an event to the system saying the job is done.
     */
    @Override
    public void run() {
        try {
            // Blocks until result
            this.processChecker.checkProcess();
            log.debug("Job {} is still running...", this.execution.getId());
            if (this.errorCount != 0) {
                this.errorCount = 0;
            }
            this.successfulCheckRate.increment();
        } catch (final GenieTimeoutException gte) {
            log.info("Job {} has timed out", this.execution.getId(), gte);
            this.timeoutRate.increment();
            this.publisher.publishEvent(new KillJobEvent(this.execution.getId(), "Job exceeded timeout", this));
        } catch (final ExecuteException ee) {
            log.info("Job {} has finished", this.execution.getId());
            this.finishedRate.increment();
            this.publisher.publishEvent(new JobFinishedEvent(this.execution, this));
        } catch (final IOException ioe) {
            // Some other error
            log.error(
                "Some IOException happened unable to check process status for pid {}",
                this.execution.getProcessId(),
                ioe
            );
            this.errorCount++;
            this.unsuccessfulCheckRate.increment();
            // If this keeps throwing errors out we should kill the job
            if (this.errorCount > MAX_ERRORS) {
                // TODO: What if they throw an exception?
                this.publisher.publishEvent(
                    new KillJobEvent(
                        this.execution.getId(),
                        "Couldn't check process status " + MAX_ERRORS + " consecutive times",
                        this
                    )
                );
                // Also send a job finished event
                this.publisher.publishEvent(new JobFinishedEvent(this.execution, this));
            }
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
        return this.execution.getCheckDelay();
    }
}
