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
import com.netflix.genie.common.exceptions.CloudServiceException;
import com.netflix.genie.common.model.Application;
import com.netflix.genie.common.model.Cluster;
import com.netflix.genie.common.model.Command;
import com.netflix.genie.common.model.FileAttachment;
import com.netflix.genie.common.model.Job;
import com.netflix.genie.common.model.Types.JobStatus;
import com.netflix.genie.server.jobmanager.JobManager;
import com.netflix.genie.server.jobmanager.JobMonitor;
import com.netflix.genie.server.repository.jpa.CommandRepository;
import com.netflix.genie.server.util.StringUtil;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.HttpURLConnection;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.activation.DataHandler;
import javax.inject.Inject;
import javax.inject.Named;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.transaction.annotation.Transactional;

/**
 * Implementation of job manager for Yarn Jobs.
 *
 * @author amsharma
 * @author skrishnan
 * @author bmundlapudi
 * @author tgianos
 */
@Named
@Scope("prototype")
public class YarnJobManager implements JobManager {

    private static final Logger LOG = LoggerFactory.getLogger(YarnJobManager.class);

    private final CommandRepository commandRepo;
    private final JobMonitor jobMonitor;
    private final Thread jobMonitorThread;

    /**
     * The name of the Genie job id property to be passed to all jobs.
     */
    private static final String GENIE_JOB_ID = "genie.job.id";

    /**
     * The value of the Genie job id property to pass to jobs.
     */
    private String genieJobIDProp;

    /**
     * The name of the Environment (test/prod) property to be passed to all
     * jobs.
     */
    private static final String NFLX_ENV = "netflix.environment";

    /**
     * The value of the environment to be passed to jobs.
     */
    private String netflixEnvProp;

    /**
     * The value for the Lipstick job ID, if needed.
     */
    private String lipstickUuidProp;

    /**
     * The environment variables for this job.
     */
    private final Map<String, String> env = new HashMap<String, String>();

    /**
     * Reference to the cluster configuration element to run the job on.
     */
    private Cluster cluster;

    /**
     * Default group name for job submissions.
     */
    private static final String HADOOP_GROUP_NAME = "hadoop";

    /**
     * The command-line arguments for this job.
     */
    private String[] args;

    /**
     * The job info for this job, which is persisted to the database.
     */
    private Job ji;

    /**
     * Actual command to run determined from the Command selected.
     */
    private String executable;

    /**
     * Default constructor - initializes cluster configuration and load
     * balancer.
     *
     * @param jobMonitor The job monitor object to use.
     * @param commandRepo The command repository to use.
     * @throws CloudServiceException if there is any error in initialization
     */
    @Inject
    public YarnJobManager(
            final JobMonitor jobMonitor,
            final CommandRepository commandRepo) throws CloudServiceException {
        this.jobMonitor = jobMonitor;
        this.jobMonitorThread = new Thread(this.jobMonitor);
        this.commandRepo = commandRepo;
    }

    /**
     * {@inheritDoc}
     *
     * @throws CloudServiceException
     */
    @Override
    public void setCluster(final Cluster cluster) throws CloudServiceException {
        if (cluster == null) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST, "No cluster entered.");
        }
        this.cluster = cluster;
    }

    /**
     * Initialize, and launch the job once it has been initialized.
     *
     * @param job the JobInfo object for the job to be launched
     * @throws CloudServiceException if there is any error in the job launch
     */
    @Override
    public void launch(final Job job) throws CloudServiceException {
        LOG.info("called");

        // initialize all the arguments and environment
        init(job);

        // create the ProcessBuilder for this process
        final ProcessBuilder pb = new ProcessBuilder(this.args);

        // set current working directory for the process
        final String cWorkingDir = this.env.get("BASE_USER_WORKING_DIR")
                + File.separator
                + this.ji.getId();
        final File userJobDir = new File(cWorkingDir);

        // check if working directory already exists
        if (userJobDir.exists()) {
            final String msg = "User staging directory already exists";
            this.ji.setJobStatus(JobStatus.FAILED, msg);
            LOG.error(this.ji.getStatusMsg() + ": "
                    + userJobDir.getAbsolutePath());
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        }

        // create the working directory
        final boolean resMkDir = userJobDir.mkdirs();
        if (!resMkDir) {
            String msg = "User staging directory can't be created";
            ji.setJobStatus(JobStatus.FAILED, msg);
            LOG.error(ji.getStatusMsg() + ": "
                    + userJobDir.getAbsolutePath());
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        }
        pb.directory(userJobDir);

        // copy over the attachments if they exist
        if (this.ji.getAttachments() != null) {
            for (final FileAttachment attachment : ji.getAttachments()) {
                // basic error checking
                if ((attachment.getName() == null) || (attachment.getName().isEmpty())) {
                    final String msg = "File attachment is missing required parameter name";
                    LOG.error(msg);
                    throw new CloudServiceException(
                            HttpURLConnection.HTTP_BAD_REQUEST, msg);
                }
                if (attachment.getData() == null) {
                    final String msg = "File attachment is missing required parameter data";
                    LOG.error(msg);
                    throw new CloudServiceException(
                            HttpURLConnection.HTTP_BAD_REQUEST, msg);
                }
                // good to go - copy attachments
                // not checking for 0-byte attachments - assuming they are legitimate
                FileOutputStream output = null;
                try {
                    output = new FileOutputStream(cWorkingDir + File.separator + attachment.getName());
                    final DataHandler inputHandler = attachment.getData();
                    inputHandler.writeTo(output);
                } catch (final IOException e) {
                    final String msg = "Unable to copy attachment correctly: " + attachment.getName();
                    LOG.error(msg);
                    throw new CloudServiceException(
                            HttpURLConnection.HTTP_INTERNAL_ERROR, msg, e);
                } finally {
                    if (output != null) {
                        try {
                            output.close();
                        } catch (final IOException ioe) {
                            final String msg = "Unable to close the output stream for the attachment";
                            LOG.error(msg, ioe);
                            // #TODO: Rethrow as CloudServiceException?
                        }
                    }
                }
            }
        }

        // set environment variables for the process
        final Map<String, String> pEnv = pb.environment();

        pEnv.putAll(this.env);

        LOG.info("Setting job working dir , conf dir and jar dir");
        // setup env for current job, conf and jar directories directories
        pEnv.put("CURRENT_JOB_WORKING_DIR", cWorkingDir);
        pEnv.put("CURRENT_JOB_CONF_DIR", cWorkingDir + "/conf");
        pEnv.put("CURRENT_JOB_JAR_DIR", cWorkingDir + "/jars");

        // setup the HADOOP_CONF_DIR
        pEnv.put("HADOOP_CONF_DIR", cWorkingDir + "/conf");

        int pid;
        try {
            // launch job, and get process handle
            final Process proc = pb.start();
            pid = this.getProcessId(proc);
            this.ji.setProcessHandle(pid);

            // set off monitor thread for the job
            this.jobMonitor.setJob(this.ji);
            this.jobMonitor.setProcess(proc);
            this.jobMonitor.setWorkingDir(cWorkingDir);
            this.jobMonitorThread.start();
            this.ji.setJobStatus(JobStatus.RUNNING, "Job is running");
        } catch (final IOException e) {
            final String msg = "Failed to launch the job";
            LOG.error(msg, e);
            this.ji.setJobStatus(JobStatus.FAILED, msg);
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, msg, e);
        }
        LOG.info("Successfully launched the job with PID = " + pid);
    }

    /**
     * Kill the job pointed to by the job info - this only kills the Yarn job
     * shell.
     *
     * @param job the jobInfo object for the job to be killed
     * @throws CloudServiceException if there is any error in job killing
     */
    @Override
    public void kill(final Job job) throws CloudServiceException {
        LOG.info("called");

        // basic error checking
        if (job == null) {
            String msg = "JobInfo object is null";
            LOG.error(msg);
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        }

        // check to ensure that the process id is actually set (which means job
        // was launched)
        this.ji = job;
        int processId = job.getProcessHandle();
        if (processId > 0) {
            LOG.info("Attempting to kill the process " + processId);
            try {
                String genieHome = ConfigurationManager.getConfigInstance()
                        .getString("netflix.genie.server.sys.home");
                if ((genieHome == null) || genieHome.isEmpty()) {
                    String msg = "Property netflix.genie.server.sys.home is not set correctly";
                    LOG.error(msg);
                    throw new CloudServiceException(HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
                }
                Runtime.getRuntime().exec(
                        genieHome + File.separator + "jobkill.sh " + processId);
            } catch (final CloudServiceException e) {
                String msg = "Failed to kill the job";
                LOG.error(msg, e);
                throw new CloudServiceException(
                        HttpURLConnection.HTTP_INTERNAL_ERROR, msg, e);
            } catch (final IOException e) {
                String msg = "Failed to kill the job";
                LOG.error(msg, e);
                throw new CloudServiceException(
                        HttpURLConnection.HTTP_INTERNAL_ERROR, msg, e);
            }
        } else {
            String msg = "Could not get process id";
            LOG.error(msg);
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        }
    }

    /**
     * Initializes the object with the job information and environment prior to
     * job launch This method must be called before job is launched.
     *
     * @param job the JobInfo object passed by the user
     * @throws CloudServiceException if there is an error during initialization
     */
    protected void init(final Job job) throws CloudServiceException {
        LOG.info("called");

        genieJobIDProp = GENIE_JOB_ID + "=" + job.getId();
        netflixEnvProp = NFLX_ENV + "="
                + ConfigurationManager.getConfigInstance().getString(
                        "netflix.environment");

        String lipstickUuidPropName = ConfigurationManager.getConfigInstance().
                getString("netflix.genie.server.lipstick.uuid.prop.name", "lipstick.uuid.prop.name");

        if (ConfigurationManager.getConfigInstance().getBoolean(
                "netflix.genie.server.lipstick.enable", false)) {
            lipstickUuidProp = lipstickUuidPropName + "=" + GENIE_JOB_ID;
        }

        // construct the environment variables
        initEnv(job);

        // construct command-line args
        this.args = initArgs(job);

        // save/init args, environment and jobinfo
        this.ji = job;
    }

    /**
     * Set/initialize environment variables for this job.
     *
     * @param job job info object for this job
     * @throws CloudServiceException if there is any error in initialization
     */
    @Transactional(readOnly = true)
    protected void initEnv(final Job job) throws CloudServiceException {
        LOG.info("called");

        if (job.getFileDependencies() != null
                && !job.getFileDependencies().isEmpty()) {
            this.env.put("CURRENT_JOB_FILE_DEPENDENCIES", job.getFileDependencies());
        }

        // set the hadoop-related conf files
        final Set<String> clusterConfigs = this.cluster.getConfigs();

        this.env.put("S3_CLUSTER_CONF_FILES", convertCollectionToCSV(clusterConfigs));

        Command command = null;

        // If Command Id is specified use directly.
        // If command Name is specified iterate through the commands in the cluster to get the Id that matches.
        if (StringUtils.isNotBlank(job.getCommandId())) {
            command = this.commandRepo.findOne(job.getCommandId());
        } else if (StringUtils.isNoneBlank(job.getCommandName())) {
            // Iterate through the commands the cluster supports and find the command that matches.
            // There has to be one that matches, else the getCluster wouldn't have.
            //TODO: Optimize via query
            for (final Command cce : this.cluster.getCommands()) {
                if (cce.getName().equals(job.getCommandName())) {
                    command = cce;
                    break;
                }
            }
        }

        //Avoiding NPE
        if (command == null) {
            final String msg = "No command found for params. Unable to continue.";
            LOG.error(msg);
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR,
                    msg);
        }

        // save the command name, application id and application name
        job.setCommandId(command.getId());
        job.setCommandName(command.getName());

        final Application application = command.getApplication();
        if (application != null) {
            job.setApplicationId(application.getId());
            job.setApplicationName(application.getName());

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
        job.setExecutionClusterName(cluster.getName());
        job.setExecutionClusterId(cluster.getId());

        // Get envPropertyFile for command and job and set env variable
        if (StringUtils.isNotBlank(command.getEnvPropFile())) {
            this.env.put("COMMAND_ENV_FILE", command.getEnvPropFile());
        }

        if (StringUtils.isNotBlank(job.getEnvPropFile())) {
            this.env.put("JOB_ENV_FILE", job.getEnvPropFile());
        }

        // put the user name for hadoop to use
        this.env.put("HADOOP_USER_NAME", job.getUser());

        // this is for the generic joblauncher.sh to use to create username
        // on the machine if needed
        this.env.put("USER_NAME", job.getUser());

        // add the group name
        String groupName = HADOOP_GROUP_NAME;
        if (job.getGroup() != null) {
            groupName = job.getGroup();
            this.env.put("GROUP_NAME", groupName);
        }
        this.env.put("HADOOP_GROUP_NAME", groupName);

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
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR,
                    msg);
        }
        this.env.put("XS_SYSTEM_HOME", genieHome);

        // if the cluster version is provided, overwrite the HADOOP_HOME
        // environment variable
        String hadoopHome;
        if (this.cluster.getVersion() != null) {
            String hadoopVersion = this.cluster.getVersion();
            LOG.debug("Hadoop Version of the cluster: " + hadoopVersion);

            // try exact version first
            hadoopHome = ConfigurationManager
                    .getConfigInstance()
                    .getString(
                            "netflix.genie.server.hadoop." + hadoopVersion
                            + ".home");
            // if not, trim to 3 most significant digits
            if (hadoopHome == null) {
                hadoopVersion = StringUtil.trimVersion(hadoopVersion);
                hadoopHome = ConfigurationManager.getConfigInstance()
                        .getString(
                                "netflix.genie.server.hadoop." + hadoopVersion
                                + ".home");
            }

            if (hadoopHome == null || !new File(hadoopHome).exists()) {
                String msg = "This genie instance doesn't support Hadoop version: "
                        + hadoopVersion;
                LOG.error(msg);
                throw new CloudServiceException(
                        HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
            }

            LOG.info("Overriding HADOOP_HOME from cluster config to: "
                    + hadoopHome);
            this.env.put("HADOOP_HOME", hadoopHome);
        } else {
            // set the default hadoop home
            hadoopHome = ConfigurationManager
                    .getConfigInstance()
                    .getString("netflix.genie.server.hadoop.home");
            if (hadoopHome == null || !new File(hadoopHome).exists()) {
                final String msg = "Property netflix.genie.server.hadoop.home is not set correctly";
                LOG.error(msg);
                throw new CloudServiceException(
                        HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
            }
            this.env.put("HADOOP_HOME", hadoopHome);
        }

        // populate the CP timeout and other options. Yarn jobs would use
        // hadoop fs -cp to copy files. Prepare the copy command with the combination
        // and set the COPY_COMMAND environment variable
        this.env.put("CP_TIMEOUT",
                ConfigurationManager.getConfigInstance()
                .getString("netflix.genie.server.hadoop.s3cp.timeout", "1800"));

        final String cpOpts = ConfigurationManager.getConfigInstance()
                .getString("netflix.genie.server.hadoop.s3cp.opts", "");

        final String copyCommand = hadoopHome + "/bin/hadoop fs " + cpOpts + " -cp";
        this.env.put("COPY_COMMAND", copyCommand);

        final String mkdirCommand = hadoopHome + "/bin/hadoop fs " + cpOpts + " -mkdir";
        this.env.put("MKDIR_COMMAND", mkdirCommand);

        // set the base working directory
        final String baseUserWorkingDir = ConfigurationManager
                .getConfigInstance()
                .getString("netflix.genie.server.user.working.dir");
        if (StringUtils.isBlank(baseUserWorkingDir)) {
            final String msg = "Property netflix.genie.server.user.working.dir is not set";
            LOG.error(msg);
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        } else {
            this.env.put("BASE_USER_WORKING_DIR", baseUserWorkingDir);
        }

        // set the archive location
        // unless user has explicitly requested for it to be disabled
        if (!job.isDisableLogArchival()) {
            final String s3ArchiveLocation = ConfigurationManager
                    .getConfigInstance()
                    .getString("netflix.genie.server.s3.archive.location");
            if (StringUtils.isNotBlank(s3ArchiveLocation)) {
                this.env.put("S3_ARCHIVE_LOCATION", s3ArchiveLocation);
            }
        }

        // set the variables to be added to the core-site xml. Format of this variable is:
        // key1=value1;key2=value2;key3=value3
        this.env.put("CORE_SITE_XML_ARGS",
                this.genieJobIDProp
                + ";"
                + this.netflixEnvProp
                + ";"
                + this.lipstickUuidProp);
    }

    /**
     * Converts a list of strings to a CSV.
     *
     * @param list ArrayList object contains the strings
     * @return a string containing the other strings as CSV
     */
    protected String convertCollectionToCSV(final Collection<String> list) {
        return StringUtils.join(list, ",");
    }

    /**
     * Extract/initialize command-line arguments passed by user.
     *
     * @param ji2 job info for this job
     * @return the parsed command-line arguments as an array
     * @throws CloudServiceException
     */
    protected String[] initArgs(Job ji2) throws CloudServiceException {

        LOG.info("called");

        String[] cmdArgs = StringUtil.splitCmdLine(ji2.getCommandArgs());

        String[] hArgs;
        hArgs = new String[cmdArgs.length + 2];

        // get the location where genie scripts are installed
        String genieHome = env.get("XS_SYSTEM_HOME");

        // first two args are the joblauncher and job type
        hArgs[0] = genieHome + File.separator + "joblauncher.sh";
        hArgs[1] = executable;

        System.arraycopy(cmdArgs, 0, hArgs, 2, cmdArgs.length);

        return hArgs;
    }

    /**
     * Return additional command-line arguments specific to genie.
     *
     * @return -D style params including genie job id and netflix environment
     */
    protected String[] getGenieCmdArgs() {
        if (this.lipstickUuidProp == null) {
            return new String[]{
                "-D",
                this.genieJobIDProp,
                "-D",
                this.netflixEnvProp
            };
        } else {
            return new String[]{
                "-D",
                this.genieJobIDProp,
                "-D",
                this.netflixEnvProp,
                "-D",
                this.lipstickUuidProp
            };
        }
    }

    /**
     * Get process id for the given process.
     *
     * @param proc java process object representing the Hadoop job launcher
     * @return pid for this process
     * @throws CloudServiceException if there is an error getting the process id
     */
    protected int getProcessId(final Process proc) throws CloudServiceException {
        LOG.debug("called");

        try {
            final Field f = proc.getClass().getDeclaredField("pid");
            f.setAccessible(true);
            return f.getInt(proc);
        } catch (Exception e) {
            String msg = "Can't get process id for job";
            LOG.error(msg, e);
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, msg, e);
        }
    }
}
