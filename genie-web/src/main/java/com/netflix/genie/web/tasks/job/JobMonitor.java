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
import com.netflix.genie.common.dto.JobStatusMessages;
import com.netflix.genie.common.exceptions.GenieTimeoutException;
import com.netflix.genie.web.events.GenieEventBus;
import com.netflix.genie.web.events.JobFinishedEvent;
import com.netflix.genie.web.events.JobFinishedReason;
import com.netflix.genie.web.events.KillJobEvent;
import com.netflix.genie.web.properties.JobsProperties;
import com.netflix.genie.web.tasks.GenieTaskScheduleType;
import com.netflix.genie.web.tasks.node.NodeTask;
import com.netflix.genie.web.util.ExponentialBackOffTrigger;
import com.netflix.genie.web.util.ProcessChecker;
import com.netflix.genie.web.util.UnixProcessChecker;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.Executor;
import org.apache.commons.lang3.SystemUtils;
import org.springframework.scheduling.Trigger;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.io.File;
import java.io.IOException;
import java.time.Instant;

/**
 * Given a process id this class will check if the job client process is running or not.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Slf4j
public class JobMonitor extends NodeTask {

    // How many error iterations we can handle
    // TODO: Make this a variable
    private static final int MAX_ERRORS = 5;
    private final String id;
    private final JobExecution execution;
    private final ProcessChecker processChecker;
    private final GenieEventBus genieEventBus;
    private final File stdOut;
    private final File stdErr;
    private final long maxStdOutLength;
    private final long maxStdErrLength;
    private final Trigger trigger;

    // Metrics
    private final Counter successfulCheckRate;
    private final Counter timeoutRate;
    private final Counter finishedRate;
    private final Counter unsuccessfulCheckRate;
    private final Counter stdOutTooLarge;
    private final Counter stdErrTooLarge;
    private int errorCount;

    /**
     * Constructor.
     *
     * @param execution      The job execution object including the pid
     * @param stdOut         The std out output file
     * @param stdErr         The std err output file
     * @param executor       The process executor to use
     * @param genieEventBus  The event bus implementation to use
     * @param registry       The metrics event registry
     * @param jobsProperties The properties for jobs
     */
    JobMonitor(
        @Valid final JobExecution execution,
        @NotNull final File stdOut,
        @NotNull final File stdErr,
        @NotNull final Executor executor,
        @NonNull final GenieEventBus genieEventBus,
        @NotNull final MeterRegistry registry,
        @NotNull final JobsProperties jobsProperties
    ) {
        if (!SystemUtils.IS_OS_UNIX) {
            throw new UnsupportedOperationException("Genie doesn't currently support " + SystemUtils.OS_NAME);
        }

        this.errorCount = 0;
        this.id = execution.getId().orElseThrow(IllegalArgumentException::new);
        this.execution = execution;
        this.genieEventBus = genieEventBus;

        final int processId = execution.getProcessId().orElseThrow(IllegalArgumentException::new);
        final Instant timeout = execution.getTimeout().orElseThrow(IllegalArgumentException::new);
        this.processChecker = new UnixProcessChecker(processId, executor, timeout);

        this.stdOut = stdOut;
        this.stdErr = stdErr;

        this.maxStdOutLength = jobsProperties.getMax().getStdOutSize();
        this.maxStdErrLength = jobsProperties.getMax().getStdErrSize();

        this.trigger = new ExponentialBackOffTrigger(
            ExponentialBackOffTrigger.DelayType.FROM_PREVIOUS_SCHEDULING,
            jobsProperties.getCompletionCheckBackOff().getMinInterval(),
            execution.getCheckDelay().orElse(jobsProperties.getCompletionCheckBackOff().getMaxInterval()),
            jobsProperties.getCompletionCheckBackOff().getFactor()
        );

        this.successfulCheckRate = registry.counter("genie.jobs.successfulStatusCheck.rate");
        this.timeoutRate = registry.counter("genie.jobs.timeout.rate");
        this.finishedRate = registry.counter("genie.jobs.finished.rate");
        this.unsuccessfulCheckRate = registry.counter("genie.jobs.unsuccessfulStatusCheck.rate");
        this.stdOutTooLarge = registry.counter("genie.jobs.stdOutTooLarge.rate");
        this.stdErrTooLarge = registry.counter("genie.jobs.stdErrTooLarge.rate");
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
            log.debug("Job {} is still running...", this.id);
            if (this.errorCount != 0) {
                this.errorCount = 0;
            }

            if (this.stdOut.exists() && this.stdOut.length() > this.maxStdOutLength) {
                this.genieEventBus.publishSynchronousEvent(
                    new KillJobEvent(this.id, JobStatusMessages.JOB_EXCEEDED_STDOUT_LENGTH, this)
                );
                this.stdOutTooLarge.increment();
                return;
            }

            if (this.stdErr.exists() && this.stdErr.length() > this.maxStdErrLength) {
                this.genieEventBus.publishSynchronousEvent(
                    new KillJobEvent(this.id, JobStatusMessages.JOB_EXCEEDED_STDERR_LENGTH, this)
                );
                this.stdErrTooLarge.increment();
                return;
            }

            this.successfulCheckRate.increment();
        } catch (final GenieTimeoutException gte) {
            log.info("Job {} has timed out", this.execution.getId(), gte);
            this.timeoutRate.increment();
            this.genieEventBus.publishSynchronousEvent(
                new KillJobEvent(this.id, JobStatusMessages.JOB_EXCEEDED_TIMEOUT, this)
            );
        } catch (final ExecuteException ee) {
            log.info("Job {} has finished", this.id);
            this.finishedRate.increment();
            this.genieEventBus.publishAsynchronousEvent(
                new JobFinishedEvent(
                    this.id,
                    JobFinishedReason.PROCESS_COMPLETED,
                    JobStatusMessages.PROCESS_DETECTED_TO_BE_COMPLETE,
                    this
                )
            );
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
                this.genieEventBus.publishSynchronousEvent(
                    new KillJobEvent(
                        this.id,
                        JobStatusMessages.JOB_PROCESS_NOT_FOUND,
                        this
                    )
                );
                // Also send a job finished event
                this.genieEventBus.publishAsynchronousEvent(
                    new JobFinishedEvent(
                        this.id,
                        JobFinishedReason.KILLED,
                        JobStatusMessages.JOB_PROCESS_NOT_FOUND,
                        this
                    )
                );
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GenieTaskScheduleType getScheduleType() {
        return GenieTaskScheduleType.TRIGGER;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Trigger getTrigger() {
        return trigger;
    }
}
