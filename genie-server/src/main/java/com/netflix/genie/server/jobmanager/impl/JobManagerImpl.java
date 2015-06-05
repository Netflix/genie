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

import com.netflix.config.ConfigurationManager;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.common.model.Application;
import com.netflix.genie.common.model.Cluster;
import com.netflix.genie.common.model.Command;
import com.netflix.genie.common.model.FileAttachment;
import com.netflix.genie.common.model.Job;
import com.netflix.genie.common.model.JobStatus;
import com.netflix.genie.server.jobmanager.JobManager;
import com.netflix.genie.server.jobmanager.JobMonitor;
import com.netflix.genie.server.services.CommandConfigService;
import com.netflix.genie.server.services.JobService;
import com.netflix.genie.server.util.StringUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * Generic base implementation of the JobManager interface.
 *
 * @author amsharma
 * @author skrishnan
 * @author bmundlapudi
 * @author tgianos
 */
public class JobManagerImpl implements JobManager {

    /**
     * Constant for reuse of semi-colon.
     */
    protected static final char SEMI_COLON = ';';
    /**
     * Default group name for job submissions.
     */
    protected static final String DEFAULT_GROUP_NAME = "hadoop";
    private static final Logger LOG = LoggerFactory.getLogger(JobManagerImpl.class);
    private static final String PID = "pid";

    //TODO: Move to a property file
    private static final char SPACE = ' ';
    private final JobMonitor jobMonitor;
    private final Thread jobMonitorThread;
    private final JobService jobService;
    private final CommandConfigService commandService;

    private boolean initCalled;
    private String jobDir;
    private Cluster cluster;
    private Job job;
    private Set<FileAttachment> attachments;

    /**
     * Default constructor - initializes cluster configuration and load
     * balancer.
     *
     * @param jobMonitor     The job monitor object to use.
     * @param jobService     The job service to use.
     * @param commandService The command service to use.
     */
    public JobManagerImpl(final JobMonitor jobMonitor,
                          final JobService jobService,
                          final CommandConfigService commandService) {
        this.jobMonitor = jobMonitor;
        this.jobMonitorThread = new Thread(this.jobMonitor);
        this.jobService = jobService;
        this.commandService = commandService;
        this.initCalled = false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void init(final Job job, final Cluster cluster) throws GenieException {
        if (job == null) {
            throw new GeniePreconditionException("No job entered.");
        }
        if (cluster == null) {
            throw new GeniePreconditionException("No cluster entered.");
        }

        //TODO: Get rid of this circular dependency
        this.jobMonitor.setJobManager(this);
        this.job = job;
        this.cluster = cluster;
        this.attachments = this.job.getAttachments();

        // save the cluster name and id
        this.jobService.setClusterInfoForJob(this.job.getId(), this.cluster.getId(), this.cluster.getName());

        // Find the command for the job
        Command command = null;
        for (final Command cmd : this.cluster.getCommands()) {
            if (cmd.getTags().containsAll(this.job.getCommandCriteria())) {
                command = cmd;
                break;
            }
        }

        //Avoiding NPE
        if (command == null) {
            final String msg = "No command found for params. Unable to continue.";
            LOG.error(msg);
            throw new GeniePreconditionException(msg);
        }

        // save the command name, application id and application name
        this.jobService.setCommandInfoForJob(this.job.getId(), command.getId(), command.getName());

        final Application application = command.getApplication();
        if (application != null) {
            this.jobService.setApplicationInfoForJob(this.job.getId(), application.getId(), application.getName());
        }

        // Refresh the job in memory to get the changes
        this.job = this.jobService.getJob(this.job.getId());

        this.initCalled = true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void launch() throws GenieException {
        LOG.info("called");
        if (!this.initCalled) {
            throw new GeniePreconditionException("Init wasn't called. Unable to continue.");
        }

        // create the ProcessBuilder for this process
        final List<String> processArgs = this.createBaseProcessArguments();
        processArgs.addAll(Arrays.asList(StringUtil.splitCmdLine(this.job.getCommandArgs())));
        final ProcessBuilder processBuilder = new ProcessBuilder(processArgs);

        // construct the environment variables
        this.setupCommonProcess(processBuilder);

        // Launch the actual process
        this.launchProcess(processBuilder, 5000);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void kill() throws GenieException {
        LOG.info("called");
        if (!this.initCalled) {
            throw new GeniePreconditionException("Init wasn't called. Unable to continue.");
        }

        // check to ensure that the process id is actually set (which means job
        // was launched)
        final int processId = this.job.getProcessHandle();
        if (processId > 0) {
            LOG.info("Attempting to kill the process " + processId);
            try {
                final String genieHome = ConfigurationManager.getConfigInstance()
                        .getString("com.netflix.genie.server.sys.home");
                if (genieHome == null || genieHome.isEmpty()) {
                    final String msg = "Property com.netflix.genie.server.sys.home is not set correctly";
                    LOG.error(msg);
                    throw new GenieServerException(msg);
                }
                Process killProcessId = Runtime.getRuntime().exec(
                        genieHome + File.separator + "jobkill.sh " + processId);

                int returnCode = 1;
                int counter = 0;
                while (counter < 3) {
                    counter++;
                    try {
                        returnCode = killProcessId.exitValue();
                        LOG.info("Kill script finished");
                        break;
                    } catch (IllegalThreadStateException e) {
                        LOG.info("Kill script not finished yet. Will retry");
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e1) {
                            LOG.info("Sleep interrupted. Ignoring.");
                        }
                    }
                }
                if (returnCode != 0) {
                    throw new GenieServerException("Failed to kill the job");
                }
            } catch (final GenieException | IOException e) {
                final String msg = "Failed to kill the job";
                LOG.error(msg, e);
                throw new GenieServerException(msg, e);
            }
        } else {
            final String msg = "Could not get process id";
            LOG.error(msg);
            throw new GenieServerException(msg);
        }
    }

    /**
     * Extract/initialize command-line arguments passed by user.
     *
     * @return the parsed command-line arguments as an array
     * @throws GenieException On issue
     */
    protected List<String> createBaseProcessArguments() throws GenieException {
        LOG.info("called");

        final List<String> processArgs = new ArrayList<>();

        // first two args are the job launcher script and job type
        processArgs.add(getGenieHome() + File.separator + "joblauncher.sh");
        processArgs.add(this.commandService.getCommand(this.job.getCommandId()).getExecutable());

        return processArgs;
    }

    /**
     * Actually launch a process based on the process builder.
     *
     * @param processBuilder The process builder to use.
     * @param sleepTime      The time to sleep between checks of the job process status
     * @throws GenieException If any issue happens launching the process.
     */
    protected void launchProcess(final ProcessBuilder processBuilder, final int sleepTime) throws GenieException {
        try {
            // launch job, and get process handle
            final Process proc = processBuilder.start();
            final int pid = this.getProcessId(proc);
            this.jobService.setProcessIdForJob(this.job.getId(), pid);

            // set off monitor thread for the job
            this.jobMonitor.setJob(this.job);
            this.jobMonitor.setProcess(proc);
            this.jobMonitor.setWorkingDir(this.jobDir);
            this.jobMonitor.setThreadSleepTime(sleepTime);
            this.jobMonitorThread.start();
            this.jobService.setJobStatus(this.job.getId(), JobStatus.RUNNING, "Job is running");
            LOG.info("Successfully launched the job with PID = " + pid);
        } catch (final IOException e) {
            final String msg = "Failed to launch the job";
            LOG.error(msg, e);
            this.jobService.setJobStatus(this.job.getId(), JobStatus.FAILED, msg);
            throw new GenieServerException(msg, e);
        }
    }

    /**
     * Check whether init was called or not.
     *
     * @return True if init was called.
     */
    protected boolean isInitCalled() {
        return this.initCalled;
    }

    /**
     * Get the cluster being used for the job.
     *
     * @return The cluster
     */
    protected Cluster getCluster() {
        return this.cluster;
    }

    /**
     * Get the job being managed.
     *
     * @return The job.
     */
    protected Job getJob() {
        return this.job;
    }

    /**
     * Get the directory used to store this jobs files and configurations.
     *
     * @return The job directory.
     */
    protected String getJobDir() {
        return this.jobDir;
    }

    /**
     * Get the group to use.
     *
     * @return The group.
     */
    protected String getGroupName() {
        if (this.job != null && this.job.getGroup() != null) {
            return this.job.getGroup();
        } else {
            return DEFAULT_GROUP_NAME;
        }
    }

    /**
     * Set/initialize environment variables for this job.
     *
     * @param processBuilder The process builder to use
     * @throws GenieException if there is any error in initialization
     */
    protected void setupCommonProcess(final ProcessBuilder processBuilder) throws GenieException {
        LOG.info("called");

        //Get the directory to stage all the work out of
        final String baseUserWorkingDir = this.getBaseUserWorkingDirectory();

        //Save the base user working directory
        processBuilder.environment().put("BASE_USER_WORKING_DIR", baseUserWorkingDir);

        //Set the process working directory
        processBuilder.directory(this.createWorkingDirectory(baseUserWorkingDir));

        //Copy any attachments from the job.
        this.copyAttachments();

        LOG.info("Setting job working dir , conf dir and jar dir");
        // setup env for current job, conf and jar directories directories
        processBuilder.environment().put("CURRENT_JOB_WORKING_DIR", this.jobDir);
        processBuilder.environment().put("CURRENT_JOB_CONF_DIR", this.jobDir + "/conf");
        processBuilder.environment().put("CURRENT_JOB_JAR_DIR", this.jobDir + "/jars");

        if (this.job.getFileDependencies() != null
                && !this.job.getFileDependencies().isEmpty()) {
            processBuilder.environment().put(
                    "CURRENT_JOB_FILE_DEPENDENCIES",
                    StringUtils.replaceChars(this.job.getFileDependencies(), ',', ' ')
            );
        }

        // set the cluster related conf files
        processBuilder.environment()
                .put("S3_CLUSTER_CONF_FILES", convertCollectionToString(this.cluster.getConfigs()));

        this.setCommandAndApplicationForJob(processBuilder);

        if (StringUtils.isNotBlank(this.job.getEnvPropFile())) {
            processBuilder.environment().put("JOB_ENV_FILE", this.job.getEnvPropFile());
        }

        // this is for the generic joblauncher.sh to use to create username
        // on the machine if needed
        processBuilder.environment().put("USER_NAME", this.job.getUser());

        processBuilder.environment().put("GROUP_NAME", this.getGroupName());

        // set the java home
        final String javaHome = ConfigurationManager
                .getConfigInstance()
                .getString("com.netflix.genie.server.java.home");
        if (StringUtils.isNotBlank(javaHome)) {
            processBuilder.environment().put("JAVA_HOME", javaHome);
        }

        // Set an ARN if one is available for role assumption with S3
        final String arn = ConfigurationManager.getConfigInstance().getString("com.netflix.genie.server.aws.iam.arn");
        if (StringUtils.isNotBlank(arn)) {
            processBuilder.environment().put("ARN", arn);
        }

        // set the genie home
        final String genieHome = ConfigurationManager
                .getConfigInstance()
                .getString("com.netflix.genie.server.sys.home");
        if (StringUtils.isBlank(genieHome)) {
            final String msg = "Property com.netflix.genie.server.sys.home is not set correctly";
            LOG.error(msg);
            throw new GenieServerException(msg);
        }
        processBuilder.environment().put("XS_SYSTEM_HOME", genieHome);

        // set the archive location
        // unless user has explicitly requested for it to be disabled
        if (!this.job.isDisableLogArchival()) {
            final String s3ArchiveLocation = ConfigurationManager
                    .getConfigInstance()
                    .getString("com.netflix.genie.server.s3.archive.location");
            if (StringUtils.isNotBlank(s3ArchiveLocation)) {
                processBuilder.environment().put("S3_ARCHIVE_LOCATION", s3ArchiveLocation);
            }
        }
    }

    /**
     * Get the Genie home location.
     *
     * @return The genie home location.
     * @throws GenieException When a home isn't set
     */
    private String getGenieHome() throws GenieException {
        final String genieHome = ConfigurationManager
                .getConfigInstance()
                .getString("com.netflix.genie.server.sys.home");
        if (StringUtils.isBlank(genieHome)) {
            final String msg = "Property com.netflix.genie.server.sys.home is not set correctly";
            LOG.error(msg);
            throw new GenieServerException(msg);
        }
        return genieHome;
    }

    /**
     * Copy over any attachments for the job which exist.
     *
     * @throws GenieException
     */
    private void copyAttachments() throws GenieException {
        // copy over the attachments if they exist
        if (this.attachments != null) {
            for (final FileAttachment attachment : this.attachments) {
                // basic error checking
                if (attachment.getName() == null || attachment.getName().isEmpty()) {
                    final String msg = "File attachment is missing required parameter name";
                    LOG.error(msg);
                    throw new GeniePreconditionException(msg);
                }
                if (attachment.getData() == null) {
                    final String msg = "File attachment is missing required parameter data";
                    LOG.error(msg);
                    throw new GeniePreconditionException(msg);
                }
                // good to go - copy attachments
                // not checking for 0-byte attachments - assuming they are legitimate
                try (final FileOutputStream output =
                             new FileOutputStream(this.jobDir + File.separator + attachment.getName())) {
                    output.write(attachment.getData());
                } catch (final IOException e) {
                    final String msg = "Unable to copy attachment correctly: " + attachment.getName();
                    LOG.error(msg);
                    throw new GenieServerException(msg, e);
                }
            }
        }
    }

    /**
     * Get the base user working directory.
     *
     * @return The base user working directory if one exists.
     * @throws GenieException If unable to get the property
     */
    private String getBaseUserWorkingDirectory() throws GenieException {
        final String baseUserWorkingDir = ConfigurationManager
                .getConfigInstance()
                .getString("com.netflix.genie.server.user.working.dir");
        if (StringUtils.isBlank(baseUserWorkingDir)) {
            final String msg = "Property com.netflix.genie.server.user.working.dir is not set";
            LOG.error(msg);
            throw new GenieServerException(msg);
        }
        return baseUserWorkingDir;
    }

    /**
     * Get the working directory for the job. Will create folders on the system.
     *
     * @param baseUserWorkingDir The base directory to start from.
     * @return The Java file reference to the directory that was created.
     * @throws GenieException If the directory already exists or unable to create
     */
    private File createWorkingDirectory(final String baseUserWorkingDir) throws GenieException {
        this.jobDir = baseUserWorkingDir + File.separator + this.job.getId();
        final File userJobDir = new File(this.jobDir);

        // check if working directory already exists
        if (userJobDir.exists()) {
            final String msg = "User staging directory already exists";
            this.jobService.setJobStatus(this.job.getId(), JobStatus.FAILED, msg);
            LOG.error(this.job.getStatusMsg() + ": " + userJobDir.getAbsolutePath());
            throw new GenieServerException(msg);
        }

        // create the working directory
        final boolean resMkDir = userJobDir.mkdirs();
        if (!resMkDir) {
            String msg = "User staging directory can't be created";
            this.jobService.setJobStatus(this.job.getId(), JobStatus.FAILED, msg);
            LOG.error(this.job.getStatusMsg() + ": " + userJobDir.getAbsolutePath());
            throw new GenieServerException(msg);
        }

        return userJobDir;
    }

    /**
     * Set the command and application for a given process and job.
     *
     * @param processBuilder The process builder to use.
     * @throws GenieException On an error interacting with database.
     */
    private void setCommandAndApplicationForJob(final ProcessBuilder processBuilder) throws GenieException {
        final Command command = this.commandService.getCommand(this.job.getCommandId());

        if (command.getConfigs() != null && !command.getConfigs().isEmpty()) {
            processBuilder.environment().put("S3_COMMAND_CONF_FILES", convertCollectionToString(command.getConfigs()));
        }

        if (StringUtils.isNotBlank(command.getEnvPropFile())) {
            processBuilder.environment().put("COMMAND_ENV_FILE", command.getEnvPropFile());
        }

        final Application application = command.getApplication();
        if (application != null) {
            if (application.getConfigs() != null && !application.getConfigs().isEmpty()) {
                processBuilder.environment()
                        .put("S3_APPLICATION_CONF_FILES", convertCollectionToString(application.getConfigs()));
            }

            if (application.getJars() != null && !application.getJars().isEmpty()) {
                processBuilder.environment()
                        .put("S3_APPLICATION_JAR_FILES", convertCollectionToString(application.getJars()));
            }

            if (StringUtils.isNotBlank(application.getEnvPropFile())) {
                processBuilder.environment().put("APPLICATION_ENV_FILE", application.getEnvPropFile());
            }
        }
    }

    /**
     * Converts a collection of strings to a CSV.
     *
     * @param collection Collection object contains the strings
     * @return a string containing the other strings as space delimited list
     */
    private String convertCollectionToString(final Collection<String> collection) {
        return StringUtils.join(collection, SPACE);
    }

    /**
     * Get process id for the given process.
     *
     * @param proc java process object representing the job launcher
     * @return pid for this process
     * @throws GenieException if there is an error getting the process id
     */
    private int getProcessId(final Process proc) throws GenieException {
        LOG.debug("called");

        try {
            final Field f = proc.getClass().getDeclaredField(PID);
            f.setAccessible(true);
            return f.getInt(proc);
        } catch (final IllegalAccessException | IllegalArgumentException | NoSuchFieldException | SecurityException e) {
            String msg = "Can't get process id for job";
            LOG.error(msg, e);
            throw new GenieServerException(msg, e);
        }
    }
}
