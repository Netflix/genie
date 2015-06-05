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
package com.netflix.genie.server.jobmanager.impl;

import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.server.jobmanager.JobMonitor;
import com.netflix.config.ConfigurationManager;
import com.netflix.genie.common.model.Job;
import com.netflix.genie.common.model.JobStatus;
import com.netflix.genie.server.jobmanager.JobManager;
import com.netflix.genie.server.metrics.GenieNodeStatistics;
import com.netflix.genie.server.services.ExecutionService;

import java.io.File;
import java.util.Properties;
import javax.mail.Authenticator;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import com.netflix.genie.server.services.JobService;
import org.apache.commons.configuration.AbstractConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The monitor thread that gets launched for each job.
 *
 * @author skrishnan
 * @author amsharma
 * @author tgianos
 */
public class JobMonitorImpl implements JobMonitor {

    private static final Logger LOG = LoggerFactory.getLogger(JobMonitorImpl.class);
    // interval to check status, and update in database if needed
    private static final int JOB_UPDATE_TIME_MS = 60000;
    // stdout filename
    private static final String STDOUT_FILENAME = "stdout";
    // stderr filename
    private static final String STDERR_FILENAME = "stderr";
    private final GenieNodeStatistics genieNodeStatistics;
    private final ExecutionService xs;
    private final JobService jobService;
    // max specified stdout size
    private final Long maxStdoutSize;
    // max specified stdout size
    private final Long maxStderrSize;
    // Config Instance to get all properties
    private final AbstractConfiguration config;
    private String jobId;
    private JobManager jobManager;
    // last updated time in DB
    private long lastUpdatedTimeMS;
    // the handle to the process for the running job
    private Process proc;
    // the working directory for this job
    private String workingDir;
    // the stdout for this job
    private File stdOutFile;
    // the stderr for this job
    private File stdErrFile;
    // whether this job has been terminated by the monitor thread
    private boolean terminated = false;
    private int sleepTime = 5000;

    /**
     * Constructor.
     *
     * @param xs                  The job execution service.
     * @param jobService          The job service API's to use.
     * @param genieNodeStatistics The statistics object to use
     */
    public JobMonitorImpl(
            final ExecutionService xs,
            final JobService jobService,
            final GenieNodeStatistics genieNodeStatistics
    ) {
        this.xs = xs;
        this.jobService = jobService;
        this.genieNodeStatistics = genieNodeStatistics;
        this.config = ConfigurationManager.getConfigInstance();
        this.maxStdoutSize = this.config.getLong("com.netflix.genie.job.max.stdout.size", null);
        this.maxStderrSize = this.config.getLong("com.netflix.genie.job.max.stderr.size", null);

        this.workingDir = null;
        this.proc = null;
        this.stdOutFile = null;
        this.stdErrFile = null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setJob(final Job job) throws GenieException {
        if (job == null || StringUtils.isBlank(job.getId())) {
            throw new GeniePreconditionException("No job entered.");
        }
        this.jobId = job.getId();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setWorkingDir(final String workingDir) {
        this.workingDir = workingDir;
        if (this.workingDir != null) {
            this.stdOutFile = new File(this.workingDir + File.separator + "stdout.log");
            this.stdErrFile = new File(this.workingDir + File.separator + "stderr.log");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setProcess(final Process proc) throws GenieException {
        if (proc == null) {
            throw new GeniePreconditionException("No process entered.");
        }
        this.proc = proc;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setJobManager(final JobManager jobManager) throws GenieException {
        if (jobManager == null) {
            throw new GeniePreconditionException("No job manager entered.");
        }
        this.jobManager = jobManager;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setThreadSleepTime(int sleepTime) throws GenieException {
        if (sleepTime < 1) {
            throw new GeniePreconditionException("Sleep time was less than 1. Unable to sleep that little.");
        }
        this.sleepTime = sleepTime;
    }

    /**
     * The main run method for this thread - wait till it finishes, and manage
     * job state in DB.
     */
    @Override
    public void run() {
        try {
            // wait for process to complete
            final boolean killed = this.xs.finalizeJob(this.jobId, waitForExit()) == JobStatus.KILLED;

            // Check if user email address is specified. If so
            // send an email to user about job completion.
            final String emailTo = this.jobService.getJob(this.jobId).getEmail();

            if (emailTo != null) {
                LOG.info("User email address: " + emailTo);

                if (sendEmail(emailTo, killed)) {
                    // Email sent successfully. Update success email counter
                    this.genieNodeStatistics.incrSuccessfulEmailCount();
                } else {
                    // Failed to send email. Update email failed counter
                    LOG.warn("Failed to send email.");
                    this.genieNodeStatistics.incrFailedEmailCount();
                }
            }
        } catch (final GenieException ge) {
            //TODO: Some sort of better handling.
            LOG.error(ge.getMessage(), ge);
        }
    }

    /**
     * Is the job running?
     *
     * @return true if job is running, false otherwise
     */
    private boolean isRunning() {
        try {
            this.proc.exitValue();
        } catch (final IllegalThreadStateException e) {
            return true;
        }
        return false;
    }

    /**
     * Check if it is time to update the job status.
     *
     * @return true if job hasn't been updated for configured time, false
     * otherwise
     */
    private boolean shouldUpdateJob() {
        long curTimeMS = System.currentTimeMillis();
        long timeSinceStartMS = curTimeMS - this.lastUpdatedTimeMS;

        return timeSinceStartMS >= JOB_UPDATE_TIME_MS;
    }

    /**
     * Wait until the job finishes, and then return exit code. Also ensure that
     * stdout is within the limit (if specified), and update DB status
     * periodically (as RUNNING).
     *
     * @return exit code for the job after it finishes
     * @throws GenieException on issue
     */
    private int waitForExit() throws GenieException {
        this.lastUpdatedTimeMS = System.currentTimeMillis();
        while (this.isRunning()) {
            try {
                Thread.sleep(this.sleepTime);
            } catch (final InterruptedException e) {
                LOG.error("Exception while waiting for job " + this.jobId
                        + " to finish", e);
                // move on
            }

            // update status only in JOB_UPDATE_TIME_MS intervals
            if (shouldUpdateJob()) {
                this.lastUpdatedTimeMS = this.jobService.setUpdateTime(this.jobId);

                // kill the job if it is writing out more than the max stdout/stderr limit
                // if it has been terminated already, move on and wait for it to clean up after itself
                String issueFile = null;
                if (!this.terminated) {
                    if (this.stdOutFile != null
                            && this.stdOutFile.exists()
                            && this.maxStdoutSize != null
                            && this.stdOutFile.length() > this.maxStdoutSize
                            ) {
                        issueFile = STDOUT_FILENAME;
                    } else if (
                            this.stdErrFile != null
                                    && this.stdErrFile.exists()
                                    && this.maxStderrSize != null
                                    && this.stdErrFile.length() > this.maxStderrSize
                            ) {
                        issueFile = STDERR_FILENAME;
                    }
                }

                if (issueFile != null) {
                    LOG.warn("Killing job " + this.jobId + " as its " + issueFile + " is greater than limit");
                    // kill the job - no need to update status, as it will be updated during next iteration
                    try {
                        this.jobManager.kill();
                        this.terminated = true;
                    } catch (final GenieException e) {
                        LOG.error("Can't kill job " + this.jobId
                                + " after exceeding " + issueFile + " limit", e);
                        // continue - hoping that it can get cleaned up during next iteration
                    }
                }
            }
        }

        return this.proc.exitValue();
    }

    /**
     * Check the properties file to figure out if an email needs to be sent at
     * the end of the job. If yes, get mail properties and try and send email
     * about Job Status.
     *
     * @return 0 for success, -1 for failure
     * @throws GenieException on issue
     */
    private boolean sendEmail(String emailTo, boolean killed) throws GenieException {
        LOG.debug("called");
        final Job job = this.jobService.getJob(this.jobId);

        if (!this.config.getBoolean("com.netflix.genie.server.mail.enable", false)) {
            LOG.warn("Email is disabled but user has specified an email address.");
            return false;
        }

        // Sender's email ID
        String fromEmail = this.config.getString(
                "com.netflix.genie.server.mail.smpt.from",
                "no-reply-genie@geniehost.com"
        );
        LOG.info("From email address to use to send email: "
                + fromEmail);

        // Set the smtp server hostname. Use localhost as default
        String smtpHost = this.config.getString("com.netflix.genie.server.mail.smtp.host", "localhost");
        LOG.debug("Email smtp server: "
                + smtpHost);

        // Get system properties
        Properties properties = new Properties();

        // Setup mail server
        properties.setProperty("mail.smtp.host", smtpHost);

        // check whether authentication should be turned on
        Authenticator auth = null;

        if (this.config.getBoolean("com.netflix.genie.server.mail.smtp.auth", false)) {
            LOG.debug("Email Authentication Enabled");

            properties.put("mail.smtp.starttls.enable", "true");
            properties.put("mail.smtp.auth", "true");

            String userName = config.getString("com.netflix.genie.server.mail.smtp.user");
            String password = config.getString("com.netflix.genie.server.mail.smtp.password");

            if (userName == null || password == null) {
                LOG.error("Authentication is enabled and username/password for smtp server is null");
                return false;
            }
            LOG.debug("Constructing authenticator object with username"
                    + userName
                    + " and password "
                    + password);
            auth = new SMTPAuthenticator(userName,
                    password);
        } else {
            LOG.debug("Email authentication not enabled.");
        }

        // Get the default Session object.
        Session session = Session.getInstance(properties, auth);

        try {
            // Create a default MimeMessage object.
            MimeMessage message = new MimeMessage(session);

            // Set From: header field of the header.
            message.setFrom(new InternetAddress(fromEmail));

            // Set To: header field of the header.
            message.addRecipient(Message.RecipientType.TO,
                    new InternetAddress(emailTo));

            JobStatus jobStatus;

            if (killed) {
                jobStatus = JobStatus.KILLED;
            } else {
                jobStatus = job.getStatus();
            }

            // Set Subject: header field
            message.setSubject("Genie Job "
                    + job.getName()
                    + " completed with Status: "
                    + jobStatus);

            // Now set the actual message
            String body = "Your Genie Job is complete\n\n"
                    + "Job ID: "
                    + job.getId()
                    + "\n"
                    + "Job Name: "
                    + job.getName()
                    + "\n"
                    + "Status: "
                    + job.getStatus()
                    + "\n"
                    + "Status Message: "
                    + job.getStatusMsg()
                    + "\n"
                    + "Output Base URL: "
                    + job.getOutputURI()
                    + "\n";

            message.setText(body);

            // Send message
            Transport.send(message);
            LOG.info("Sent email message successfully....");
            return true;
        } catch (final MessagingException mex) {
            LOG.error("Got exception while sending email", mex);
            return false;
        }
    }

    private static class SMTPAuthenticator extends Authenticator {

        private final String username;
        private final String password;

        /**
         * Default constructor.
         */
        SMTPAuthenticator(final String username, final String password) {
            this.username = username;
            this.password = password;
        }

        /**
         * Return a PasswordAuthentication object based on username/password.
         */
        @Override
        public PasswordAuthentication getPasswordAuthentication() {
            return new PasswordAuthentication(this.username, this.password);
        }
    }
}
