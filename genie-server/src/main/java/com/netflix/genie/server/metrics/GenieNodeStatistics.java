/*
 *
 *  Copyright 2014 Netflix, Inc.
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
package com.netflix.genie.server.metrics;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Singleton class that implements all servo monitoring for Genie.
 *
 * @author skrishnan
 * @author tgianos
 */
public interface GenieNodeStatistics {

    /**
     * Register static instance with servo.
     */
    void register();

    /**
     * Get count of 2xx responses.
     *
     * @return count of 2xx responses
     */
    AtomicLong getGenie2xxCount();

    /**
     * Increment 2xx response count atomically.
     */
    void incrGenie2xxCount();

    /**
     * Get count of 4xx responses.
     *
     * @return count of 4xx responses
     */
    AtomicLong getGenie4xxCount();

    /**
     * Increment 4xx response count atomically.
     */
    void incrGenie4xxCount();

    /**
     * Get count of 5xx responses.
     *
     * @return count of 5xx responses
     */
    AtomicLong getGenie5xxCount();

    /**
     * Increment 5xx response count atomically.
     */
    void incrGenie5xxCount();

    /**
     * Get number of job submissions on this instance.
     *
     * @return number of job submissions on this instance
     */
    AtomicLong getGenieJobSubmissions();

    /**
     * Increment job submissions on this instance atomically.
     */
    void incrGenieJobSubmissions();

    /**
     * Get number of successful email sent from this instance.
     *
     * @return number of successful email sent from this instance
     */
    AtomicLong getSuccessfulEmailSentCount();

    /**
     * Increment successful email sent from this instance atomically.
     */
    void incrSuccessfulEmailCount();

    /**
     * Get number job submission retries.
     *
     * @return number of job submission retries for this instance.
     */
    AtomicLong getJobSubmissionRetryCount();

    /**
     * Increment job submission retry count.
     */
    void incrJobSubmissionRetryCount();

    /**
     * Get number of email that failed to be sent from this instance.
     *
     * @return number of failed email attempts on this instance
     */
    AtomicLong getFailedEmailSentCount();

    /**
     * Increment failed email sent count on this instance atomically.
     */
    void incrFailedEmailCount();

    /**
     * Get number of successful jobs on this instance.
     *
     * @return number of successful jobs on this instance
     */
    AtomicLong getGenieSuccessfulJobs();

    /**
     * Increment number of successful jobs atomically.
     */
    void incrGenieSuccessfulJobs();

    /**
     * Get number of failed jobs on this instance.
     *
     * @return number of failed jobs on this instance
     */
    AtomicLong getGenieFailedJobs();

    /**
     * Increment number of failed jobs atomically.
     */
    void incrGenieFailedJobs();

    /**
     * Get number of forwarded jobs from this instance.
     *
     * @return number of forwarded jobs from this instance
     */
    AtomicLong getGenieForwardedJobs();

    /**
     * Increment number of forwarded jobs atomically.
     */
    void incrGenieForwardedJobs();

    /**
     * Get number of killed jobs on this instance.
     *
     * @return number of killed jobs on this instance
     */
    AtomicLong getGenieKilledJobs();

    /**
     * Increment number of killed jobs atomically.
     */
    void incrGenieKilledJobs();

    /**
     * A setter method for 2xx count.
     *
     * @param genie2xxCount long value to set
     */
    void setGenie2xxCount(AtomicLong genie2xxCount);

    /**
     * A setter method for 4xx count.
     *
     * @param genie4xxCount long value to set
     */
    void setGenie4xxCount(AtomicLong genie4xxCount);

    /**
     * A setter method for 5xx count.
     *
     * @param genie5xxCount long value to set
     */
    void setGenie5xxCount(AtomicLong genie5xxCount);

    /**
     * A setter method for job submissions.
     *
     * @param genieJobSubmissions long value to set
     */
    void setGenieJobSubmissions(AtomicLong genieJobSubmissions);

    /**
     * A setter method for successful jobs.
     *
     * @param genieSuccessfulJobs long value to set
     */
    void setGenieSuccessfulJobs(AtomicLong genieSuccessfulJobs);

    /**
     * A setter method for forwarded jobs.
     *
     * @param genieForwardedJobs long value to set
     */
    void setGenieForwardedJobs(AtomicLong genieForwardedJobs);

    /**
     * A setter method for failed jobs.
     *
     * @param genieFailedJobs long value to set
     */
    void setGenieFailedJobs(AtomicLong genieFailedJobs);

    /**
     * A setter method for killed jobs.
     *
     * @param genieKilledJobs long value to set
     */
    void setGenieKilledJobs(AtomicLong genieKilledJobs);

    /**
     * Get number of running jobs on this instance.
     *
     * @return number of running jobs on this instance
     */
    AtomicInteger getGenieRunningJobs();

    /**
     * Set number of running jobs on this instance.
     *
     * @param genieRunningJobs number of running jobs on this instance
     */
    void setGenieRunningJobs(int genieRunningJobs);

    /**
     * Get number of running jobs with runtime less than 15 mins.
     *
     * @return number of running jobs with runtime less than 15 mins
     */
    AtomicInteger getGenieRunningJobs0To15m();

    /**
     * Set the number of running jobs with runtime less than 15 mins.
     *
     * @param genieRunningJobs0To15m number of running jobs with runtime less
     *                               than 15 mins
     */
    void setGenieRunningJobs0To15m(int genieRunningJobs0To15m);

    /**
     * Get the number of running jobs with runtime between 15 mins and 2 hours.
     *
     * @return number of running jobs with runtime between 15 mins and 2 hours
     */
    AtomicInteger getGenieRunningJobs15mTo2h();

    /**
     * Set the number of running jobs with runtime between 15 mins and 2 hours.
     *
     * @param genieRunningJobs15mTo2h number of running jobs with runtime
     *                                between 15 mins and 2 hours
     */
    void setGenieRunningJobs15mTo2h(int genieRunningJobs15mTo2h);

    /**
     * Get the number of running jobs with runtime between 2 to 8 hours.
     *
     * @return number of running jobs with runtime between 2 to 8 hours.
     */
    AtomicInteger getGenieRunningJobs2hTo8h();

    /**
     * Set the number of running jobs with runtime between 2 to 8 hours.
     *
     * @param genieRunningJobs2hTo8h number of running jobs with runtime between
     *                               2 to 8 hours.
     */
    void setGenieRunningJobs2hTo8h(int genieRunningJobs2hTo8h);

    /**
     * Get the number of running jobs with runtime greater than 8 hours.
     *
     * @return number of running jobs with runtime greater than 8 hours
     */
    AtomicInteger getGenieRunningJobs8hPlus();

    /**
     * Set the number of running jobs with runtime greater than 8 hours.
     *
     * @param genieRunningJobs8hPlus number of running jobs with runtime greater
     *                               than 8 hours
     */
    void setGenieRunningJobs8hPlus(int genieRunningJobs8hPlus);

    /**
     * Shut down cleanly.
     */
    void shutdown();
}
