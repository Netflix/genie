package com.netflix.genie.core.services;

import com.netflix.genie.common.dto.Application;
import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.exceptions.GenieException;

import java.util.List;

/**
 * A service which defines the three basic stages of a job.
 *
 * @author amajumdar
 * @since 3.0.0
 */
public interface JobStateService extends JobMetricsService {
    /**
     * Initialize the job.
     * @param jobId job id
     */
    void init(final String jobId);

    /**
     * Schedules the job.
     *
     * @param jobId         job id
     * @param jobRequest    job request
     * @param cluster       cluster for the job request based on the tags specified
     * @param command       command for the job request based on command tags and cluster chosen
     * @param applications  applications to use based on the command that was selected
     * @param memory        job memory
     *
     */
    void schedule(final String jobId, final JobRequest jobRequest, final Cluster cluster, final Command command,
                  final List<Application> applications, final int memory);

    /**
     * Called when the job is done.
     * @param jobId job id
     * @throws GenieException on unrecoverable error
     */
    void done(final String jobId) throws GenieException;

    /**
     * Returns true if the job exists locally.
     * @param jobId job id
     * @return true if job exists
     */
    boolean jobExists(final String jobId);
}
