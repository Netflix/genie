/*
 *
 *  Copyright 2013 Netflix, Inc.
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

package com.netflix.genie.server.jobmanager.impl;

import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.genie.common.model.JobInfoElement;
import com.netflix.genie.common.model.Types;
import com.netflix.genie.common.model.Types.JobStatus;
import com.netflix.genie.common.model.Types.SubprocessStatus;
import com.netflix.genie.server.metrics.GenieNodeStatistics;
import com.netflix.genie.server.persistence.PersistenceManager;
import com.netflix.genie.server.util.NetUtil;

/**
 * The monitor thread that gets launched for each job.
 *
 * @author skrishnan
 */
public class JobMonitor extends Thread {
    private static Logger logger = LoggerFactory.getLogger(JobMonitor.class);

    private JobInfoElement ji;
    private PersistenceManager<JobInfoElement> pm;

    // interval to poll for process status
    private static final int JOB_WAIT_TIME_MS = 5000;

    // interval to update status in database
    private static final int JOB_DB_UPDATE_TIME_MS = 60000;

    // last updated time in DB
    private long lastUpdatedTimeMS;

    // the handle to the process for the running job
    private Process proc;

    /**
     * Initialize this monitor thread with the process for the running job.
     *
     * @param proc
     *            process handle for running job
     */
    public JobMonitor(JobInfoElement ji, Process proc) {
        this.ji = ji;
        this.proc = proc;
        this.pm = new PersistenceManager<JobInfoElement>();
    }

    /**
     * Is the job running?
     *
     * @return true if job is running, false otherwise
     */
    private boolean isRunning() {
        try {
            proc.exitValue();
        } catch (IllegalThreadStateException e) {
            return true;
        }
        return false;
    }

    /**
     * Check if it is time to update the database.
     *
     * @return true if db hasn't been updated for configured time, false
     *         otherwise
     */
    private boolean shouldUpdateDB() {
        long curTimeMS = System.currentTimeMillis();
        long timeSinceStartMS = curTimeMS - lastUpdatedTimeMS;

        if (timeSinceStartMS >= JOB_DB_UPDATE_TIME_MS) {
            return true;
        }

        return false;
    }

    /**
     * Wait until the job finishes, and then return exit code Also update DB
     * status periodically (as RUNNING).
     *
     * @return exit code for the job after it finishes
     */
    private int waitForExit() {
        lastUpdatedTimeMS = System.currentTimeMillis();
        while (isRunning()) {
            try {
                Thread.sleep(JOB_WAIT_TIME_MS);
            } catch (InterruptedException e) {
                logger.error("Exception while waiting for job " + ji.getJobID()
                        + " to finish", e);
                // move on
            }

            // update database only in JOB_DB_UPDATE_TIME_SEC intervals
            if (shouldUpdateDB()) {
                logger.debug("updating db for job: " + ji.getJobID());

                lastUpdatedTimeMS = System.currentTimeMillis();
                ji.setJobStatus(JobStatus.RUNNING, "Job is running");
                ji.setUpdateTime(lastUpdatedTimeMS);

                // only update if it is not KILLED already
                ReentrantReadWriteLock rwl = PersistenceManager.getDbLock();
                try {
                    rwl.writeLock().lock();
                    JobInfoElement dbJI = pm.getEntity(ji.getJobID(),
                            JobInfoElement.class);
                    if ((dbJI.getStatus() != null)
                            && !dbJI.getStatus().equalsIgnoreCase("KILLED")) {
                        pm.updateEntity(ji);
                    }
                } catch (Exception e) {
                    logger.error(
                            "Exception while trying to update status for job: "
                                    + ji.getJobID(), e);
                    // continue - as we shouldn't terminate this thread until
                    // job is running
                    continue;
                } finally {
                    // ensure that we always unlock
                    if (rwl.writeLock().isHeldByCurrentThread()) {
                        rwl.writeLock().unlock();
                    }
                }
            }
        }
        return proc.exitValue();
    }

    /**
     * The main run method for this thread - wait till it finishes, and manage
     * job state in DB.
     */
    @Override
    public void run() {
        // wait for process to complete
        int exitCode = waitForExit();
        ji.setExitCode(exitCode);

        ReentrantReadWriteLock rwl = PersistenceManager.getDbLock();
        try {
            // get job status from the DB to ensure it was not killed
            // acquire a write lock first
            rwl.writeLock().lock();

            JobInfoElement dbJI = pm.getEntity(ji.getJobID(),
                    JobInfoElement.class);

            // only update status if not KILLED
            if ((dbJI.getStatus() != null)
                    && !dbJI.getStatus().equalsIgnoreCase("KILLED")) {
                GenieNodeStatistics stats = GenieNodeStatistics.getInstance();
                if (exitCode != SubprocessStatus.SUCCESS.code()) {
                    // all other failures except s3 log archival failure
                    logger.error("Failed to execute job, exit code: "
                            + exitCode);
                    String errMsg = Types.SubprocessStatus.message(exitCode);
                    if ((errMsg == null) || (errMsg.isEmpty())) {
                        errMsg = "Please look at job's stderr for more details";
                    }
                    ji.setJobStatus(JobStatus.FAILED,
                            "Failed to execute job, Error Message: " + errMsg);
                    // incr counter for failed jobs
                    stats.incrGenieFailedJobs();
                } else {
                    // success
                    ji.setJobStatus(JobStatus.SUCCEEDED,
                            "Job finished successfully");
                    // incr counter for successful jobs
                    stats.incrGenieSuccessfulJobs();
                }

                // set the archive location
                ji.setArchiveLocation(NetUtil.getArchiveURI(ji.getJobID()));

                // update the job status
                pm.updateEntity(ji);
                rwl.writeLock().unlock();
                return;
            } else {
                // if job status is killed, the kill thread will update status
                logger.debug("Job has been killed - will not update DB: "
                        + ji.getJobID());
                rwl.writeLock().unlock();
                return;
            }
        } finally {
            // ensure that we always unlock
            if (rwl.writeLock().isHeldByCurrentThread()) {
                rwl.writeLock().unlock();
            }
        }
    }
}
