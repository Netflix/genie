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
package com.netflix.genie.server.jobmanager.impl;

import com.netflix.config.ConfigurationManager;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.common.model.*;
import com.netflix.genie.server.jobmanager.JobManager;
import com.netflix.genie.server.jobmanager.JobMonitor;
import com.netflix.genie.server.services.JobService;
import com.netflix.genie.server.util.StringUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Scope;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Generic base implementation of the JobManager interface.
 *
 * @author amsharma
 * @author skrishnan
 * @author bmundlapudi
 * @author tgianos
 */
@Named
@Scope("prototype")
public class JobManagerImpl implements JobManager {

    private static final Logger LOG = LoggerFactory.getLogger(JobManagerImpl.class);
    private static final String PID = "pid";
    private static final char COMMA = ',';

    /**
     * Constant for reuse of semi-colon.
     */
    protected static final char SEMI_COLON = ';';

    /**
     * The name of the Genie job id property to be passed to all jobs.
     */
    protected static final String GENIE_JOB_ID = "genie.job.id";

    //TODO: This is netflix specific
    /**
     * The name of the Environment (test/prod) property to be passed to all
     * jobs.
     */
    protected static final String NETFLIX_ENV = "netflix.environment";

    /**
     * Default group name for job submissions.
     */
    protected static final String DEFAULT_GROUP_NAME = "hadoop";

    private final Map<String, String> env = new HashMap<>();
    private String executable;
    private Cluster cluster;
    private Job job;

    private final JobMonitor jobMonitor;
    private final Thread jobMonitorThread;
    private final JobService jobService;

    private boolean initCalled;
    private String jobDir;
    private ProcessBuilder processBuilder;

    /**
     * Default constructor - initializes cluster configuration and load
     * balancer.
     *
     * @param jobMonitor The job monitor object to use.
     * @param jobService The job service to use.
     */
    @Inject
    public JobManagerImpl(final JobMonitor jobMonitor,
                          final JobService jobService) {
        this.jobMonitor = jobMonitor;
        this.jobMonitorThread = new Thread(this.jobMonitor);
        this.jobService = jobService;
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

        // construct the environment variables
        this.initEnv();

        this.initJobProcess();

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

        try {
            // launch job, and get process handle
            final Process proc = this.processBuilder.start();
            final int pid = this.getProcessId(proc);
            this.jobService.setProcessIdForJob(this.job.getId(), pid);

            // set off monitor thread for the job
            this.jobMonitor.setJob(this.job);
            this.jobMonitor.setProcess(proc);
            this.jobMonitor.setWorkingDir(this.jobDir);
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
     * {@inheritDoc}
     */
    @Override
    public void kill() throws GenieException {
        LOG.info("called");

        // basic error checking
        if (this.job == null) {
            final String msg = "JobInfo object is null";
            LOG.error(msg);
            throw new GeniePreconditionException(msg);
        }

        // check to ensure that the process id is actually set (which means job
        // was launched)
        final int processId = this.job.getProcessHandle();
        if (processId > 0) {
            LOG.info("Attempting to kill the process " + processId);
            try {
                final String genieHome = ConfigurationManager.getConfigInstance()
                        .getString("netflix.genie.server.sys.home");
                if (genieHome == null || genieHome.isEmpty()) {
                    final String msg = "Property netflix.genie.server.sys.home is not set correctly";
                    LOG.error(msg);
                    throw new GenieServerException(msg);
                }
                Runtime.getRuntime().exec(
                        genieHome + File.separator + "jobkill.sh " + processId);
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
     * Get the process builder being used to build the job.
     *
     * @return the process builder.
     */
    protected ProcessBuilder getProcessBuilder() {
        return this.processBuilder;
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
     * @throws GenieException if there is any error in initialization
     */
    private void initEnv() throws GenieException {
        LOG.info("called");

        if (this.job.getFileDependencies() != null
                && !this.job.getFileDependencies().isEmpty()) {
            this.env.put("CURRENT_JOB_FILE_DEPENDENCIES", this.job.getFileDependencies());
        }

        // set the cluster related conf files
        final Set<String> clusterConfigs = this.cluster.getConfigs();

        this.env.put("S3_CLUSTER_CONF_FILES", convertCollectionToCSV(clusterConfigs));

        Command command = null;
        for (final Command cmd : this.cluster.getCommands()) {
            if (cmd.getTags().containsAll(job.getCommandCriteria())) {
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

            if (application.getConfigs() != null && !application.getConfigs().isEmpty()) {
                this.env.put("S3_APPLICATION_CONF_FILES", convertCollectionToCSV(application.getConfigs()));
            }

            if (application.getJars() != null && !application.getJars().isEmpty()) {
                this.env.put("S3_APPLICATION_JAR_FILES", convertCollectionToCSV(application.getJars()));
            }

            if (StringUtils.isNotBlank(application.getEnvPropFile())) {
                this.env.put("APPLICATION_ENV_FILE", application.getEnvPropFile());
            }
        }

        //Command ce = pmCommand.getEntity(cmdId, Command.class);
        if (command.getConfigs() != null && !command.getConfigs().isEmpty()) {
            this.env.put("S3_COMMAND_CONF_FILES", convertCollectionToCSV(command.getConfigs()));
        }

        this.executable = command.getExecutable();

        // save the cluster name and id
        this.jobService.setClusterInfoForJob(this.job.getId(), this.cluster.getId(), this.cluster.getName());

        // Get envPropertyFile for command and job and set env variable
        if (StringUtils.isNotBlank(command.getEnvPropFile())) {
            this.env.put("COMMAND_ENV_FILE", command.getEnvPropFile());
        }

        if (StringUtils.isNotBlank(this.job.getEnvPropFile())) {
            this.env.put("JOB_ENV_FILE", this.job.getEnvPropFile());
        }

        // this is for the generic joblauncher.sh to use to create username
        // on the machine if needed
        this.env.put("USER_NAME", this.job.getUser());

        this.env.put("GROUP_NAME", this.getGroupName());

        // set the java home
        final String javaHome = ConfigurationManager
                .getConfigInstance()
                .getString("netflix.genie.server.java.home");
        if (StringUtils.isNotBlank(javaHome)) {
            this.env.put("JAVA_HOME", javaHome);
        }

        // set the genie home
        final String genieHome = ConfigurationManager
                .getConfigInstance()
                .getString("netflix.genie.server.sys.home");
        if (StringUtils.isBlank(genieHome)) {
            final String msg = "Property netflix.genie.server.sys.home is not set correctly";
            LOG.error(msg);
            throw new GenieServerException(msg);
        }
        this.env.put("XS_SYSTEM_HOME", genieHome);

        // set the base working directory
        final String baseUserWorkingDir = ConfigurationManager
                .getConfigInstance()
                .getString("netflix.genie.server.user.working.dir");
        if (StringUtils.isBlank(baseUserWorkingDir)) {
            final String msg = "Property netflix.genie.server.user.working.dir is not set";
            LOG.error(msg);
            throw new GenieServerException(msg);
        } else {
            this.env.put("BASE_USER_WORKING_DIR", baseUserWorkingDir);
        }

        // set the archive location
        // unless user has explicitly requested for it to be disabled
        if (!this.job.isDisableLogArchival()) {
            final String s3ArchiveLocation = ConfigurationManager
                    .getConfigInstance()
                    .getString("netflix.genie.server.s3.archive.location");
            if (StringUtils.isNotBlank(s3ArchiveLocation)) {
                this.env.put("S3_ARCHIVE_LOCATION", s3ArchiveLocation);
            }
        }
    }

    /**
     * Converts a list of strings to a CSV.
     *
     * @param list ArrayList object contains the strings
     * @return a string containing the other strings as CSV
     */
    private String convertCollectionToCSV(final Collection<String> list) {
        return StringUtils.join(list, COMMA);
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

    /**
     * Initialize the process and other things that will be used to run the job.
     *
     * @throws GenieException
     */
    private void initJobProcess() throws GenieException {
        // create the ProcessBuilder for this process
        this.processBuilder = new ProcessBuilder(this.createProcessArguments());

        // set current working directory for the process
        this.jobDir = this.env.get("BASE_USER_WORKING_DIR")
                + File.separator
                + this.job.getId();
        final File userJobDir = new File(this.jobDir);

        // check if working directory already exists
        if (userJobDir.exists()) {
            final String msg = "User staging directory already exists";
            this.jobService.setJobStatus(this.job.getId(), JobStatus.FAILED, msg);
            LOG.error(this.job.getStatusMsg() + ": "
                    + userJobDir.getAbsolutePath());
            throw new GenieServerException(msg);
        }

        // create the working directory
        final boolean resMkDir = userJobDir.mkdirs();
        if (!resMkDir) {
            String msg = "User staging directory can't be created";
            this.jobService.setJobStatus(this.job.getId(), JobStatus.FAILED, msg);
            LOG.error(this.job.getStatusMsg() + ": "
                    + userJobDir.getAbsolutePath());
            throw new GenieServerException(msg);
        }
        this.processBuilder.directory(userJobDir);

        // copy over the attachments if they exist
        if (this.job.getAttachments() != null) {
            for (final FileAttachment attachment : this.job.getAttachments()) {
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
                FileOutputStream output = null;
                try {
                    output = new FileOutputStream(this.jobDir + File.separator + attachment.getName());
                    output.write(attachment.getData());
                } catch (final IOException e) {
                    final String msg = "Unable to copy attachment correctly: " + attachment.getName();
                    LOG.error(msg);
                    throw new GenieServerException(msg, e);
                } finally {
                    if (output != null) {
                        try {
                            output.close();
                        } catch (final IOException ioe) {
                            final String msg = "Unable to close the output stream for the attachment";
                            LOG.error(msg, ioe);
                        }
                    }
                }
            }
        }

        // set environment variables for the process
        final Map<String, String> pEnv = this.processBuilder.environment();

        pEnv.putAll(this.env);

        LOG.info("Setting job working dir , conf dir and jar dir");
        // setup env for current job, conf and jar directories directories
        pEnv.put("CURRENT_JOB_WORKING_DIR", this.jobDir);
        pEnv.put("CURRENT_JOB_CONF_DIR", this.jobDir + "/conf");
        pEnv.put("CURRENT_JOB_JAR_DIR", this.jobDir + "/jars");
    }

    /**
     * Extract/initialize command-line arguments passed by user.
     *
     * @return the parsed command-line arguments as an array
     * @throws GenieException On issue
     */
    private String[] createProcessArguments() throws GenieException {
        LOG.info("called");

        final String[] cmdArgs = StringUtil.splitCmdLine(this.job.getCommandArgs());

        final String[] processArgs = new String[cmdArgs.length + 2];

        // get the location where genie scripts are installed
        final String genieHome = this.env.get("XS_SYSTEM_HOME");

        // first two args are the joblauncher and job type
        processArgs[0] = genieHome + File.separator + "joblauncher.sh";
        processArgs[1] = this.executable;

        System.arraycopy(cmdArgs, 0, processArgs, 2, cmdArgs.length);

        return processArgs;
    }
}
