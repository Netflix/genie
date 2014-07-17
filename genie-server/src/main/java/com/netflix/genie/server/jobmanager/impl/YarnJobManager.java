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
import com.netflix.genie.common.model.Application;
import com.netflix.genie.common.model.Cluster;
import com.netflix.genie.common.model.Command;
import com.netflix.genie.common.model.FileAttachment;
import com.netflix.genie.common.model.Job;
import com.netflix.genie.common.model.JobStatus;
import com.netflix.genie.server.jobmanager.JobManager;
import com.netflix.genie.server.jobmanager.JobMonitor;
import com.netflix.genie.server.services.ExecutionService;
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
    /**
     * The name of the Genie job id property to be passed to all jobs.
     */
    private static final String GENIE_JOB_ID = "genie.job.id";
    /**
     * The name of the Environment (test/prod) property to be passed to all
     * jobs.
     */
    private static final String NFLX_ENV = "netflix.environment";
    /**
     * Default group name for job submissions.
     */
    private static final String HADOOP_GROUP_NAME = "hadoop";
    private final JobMonitor jobMonitor;
    private final Thread jobMonitorThread;
    /**
     * The environment variables for this job.
     */
    private final Map<String, String> env = new HashMap<String, String>();
    /**
     * The value of the Genie job id property to pass to jobs.
     */
    private String genieJobIDProp;
    /**
     * The value of the environment to be passed to jobs.
     */
    private String netflixEnvProp;
    /**
     * The value for the Lipstick job ID, if needed.
     */
    private String lipstickUuidProp;
    /**
     * Reference to the cluster configuration element to run the job on.
     */
    private Cluster cluster;
    /**
     * The command-line arguments for this job.
     */
    private String[] args;

    /**
     * The job info for this job, which is persisted to the database.
     */
    private Job job;

    /**
     * Actual command to run determined from the Command selected.
     */
    private String executable;

    /**
     * Default constructor - initializes cluster configuration and load
     * balancer.
     *
     * @param jobMonitor The job monitor object to use.
     */
    @Inject
    public YarnJobManager(final JobMonitor jobMonitor, final ExecutionService xs) {
        this.jobMonitor = jobMonitor;
        this.jobMonitorThread = new Thread(this.jobMonitor);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void init(final Job job, final Cluster cluster) throws GenieException {
        if (job == null) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST, "No job entered.");
        }
        if (cluster == null) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST, "No cluster entered.");
        }
        this.jobMonitor.setJobManager(this);
        this.job = job;
        this.cluster = cluster;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional
    public void launch() throws GenieException {
        LOG.info("called");

        // initialize all the arguments and environment
        this.initLaunch();

        // create the ProcessBuilder for this process
        final ProcessBuilder pb = new ProcessBuilder(this.args);

        // set current working directory for the process
        final String cWorkingDir = this.env.get("BASE_USER_WORKING_DIR")
                + File.separator
                + this.job.getId();
        final File userJobDir = new File(cWorkingDir);

        // check if working directory already exists
        if (userJobDir.exists()) {
            final String msg = "User staging directory already exists";
            this.job.setJobStatus(JobStatus.FAILED, msg);
            LOG.error(this.job.getStatusMsg() + ": "
                    + userJobDir.getAbsolutePath());
            throw new GenieException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        }

        // create the working directory
        final boolean resMkDir = userJobDir.mkdirs();
        if (!resMkDir) {
            String msg = "User staging directory can't be created";
            job.setJobStatus(JobStatus.FAILED, msg);
            LOG.error(job.getStatusMsg() + ": "
                    + userJobDir.getAbsolutePath());
            throw new GenieException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        }
        pb.directory(userJobDir);

        // copy over the attachments if they exist
        if (this.job.getAttachments() != null) {
            for (final FileAttachment attachment : this.job.getAttachments()) {
                // basic error checking
                if ((attachment.getName() == null) || (attachment.getName().isEmpty())) {
                    final String msg = "File attachment is missing required parameter name";
                    LOG.error(msg);
                    throw new GenieException(
                            HttpURLConnection.HTTP_BAD_REQUEST, msg);
                }
                if (attachment.getData() == null) {
                    final String msg = "File attachment is missing required parameter data";
                    LOG.error(msg);
                    throw new GenieException(
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
                    throw new GenieException(
                            HttpURLConnection.HTTP_INTERNAL_ERROR, msg, e);
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
            this.job.setProcessHandle(pid);

            // set off monitor thread for the job
            this.jobMonitor.setJob(this.job);
            this.jobMonitor.setProcess(proc);
            this.jobMonitor.setWorkingDir(cWorkingDir);
            this.jobMonitorThread.start();
            this.job.setJobStatus(JobStatus.RUNNING, "Job is running");
        } catch (final IOException e) {
            final String msg = "Failed to launch the job";
            LOG.error(msg, e);
            this.job.setJobStatus(JobStatus.FAILED, msg);
            throw new GenieException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, msg, e);
        }
        LOG.info("Successfully launched the job with PID = " + pid);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional
    public void kill() throws GenieException {
        LOG.info("called");

        // basic error checking
        if (this.job == null) {
            String msg = "JobInfo object is null";
            LOG.error(msg);
            throw new GenieException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        }

        // check to ensure that the process id is actually set (which means job
        // was launched)
        int processId = this.job.getProcessHandle();
        if (processId > 0) {
            LOG.info("Attempting to kill the process " + processId);
            try {
                final String genieHome = ConfigurationManager.getConfigInstance()
                        .getString("netflix.genie.server.sys.home");
                if ((genieHome == null) || genieHome.isEmpty()) {
                    final String msg = "Property netflix.genie.server.sys.home is not set correctly";
                    LOG.error(msg);
                    throw new GenieException(HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
                }
                Runtime.getRuntime().exec(
                        genieHome + File.separator + "jobkill.sh " + processId);
            } catch (final GenieException e) {
                final String msg = "Failed to kill the job";
                LOG.error(msg, e);
                throw new GenieException(
                        HttpURLConnection.HTTP_INTERNAL_ERROR, msg, e);
            } catch (final IOException e) {
                final String msg = "Failed to kill the job";
                LOG.error(msg, e);
                throw new GenieException(
                        HttpURLConnection.HTTP_INTERNAL_ERROR, msg, e);
            }
        } else {
            final String msg = "Could not get process id";
            LOG.error(msg);
            throw new GenieException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        }
    }

    /**
     * Initializes the object with the job information and environment prior to
     * job launch This method must be called before job is launched.
     *
     * @throws GenieException if there is an error during initialization
     */
    private void initLaunch() throws GenieException {
        LOG.info("called");

        this.genieJobIDProp = GENIE_JOB_ID + "=" + job.getId();
        this.netflixEnvProp = NFLX_ENV + "="
                + ConfigurationManager.getConfigInstance().getString(
                        "netflix.environment");

        final String lipstickUuidPropName = ConfigurationManager.getConfigInstance().
                getString("netflix.genie.server.lipstick.uuid.prop.name",
                        "lipstick.uuid.prop.name");

        if (ConfigurationManager.getConfigInstance().getBoolean(
                "netflix.genie.server.lipstick.enable", false)) {
            this.lipstickUuidProp = lipstickUuidPropName + "=" + GENIE_JOB_ID;
        }

        // construct the environment variables
        this.initEnv();

        // construct command-line args
        this.args = initArgs();
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

        // set the hadoop-related conf files
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
            throw new GenieException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR,
                    msg);
        }

        // save the command name, application id and application name
        this.job.setCommandId(command.getId());
        this.job.setCommandName(command.getName());

        final Application application = command.getApplication();
        if (application != null) {
            this.job.setApplicationId(application.getId());
            this.job.setApplicationName(application.getName());

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
        this.job.setExecutionClusterName(cluster.getName());
        this.job.setExecutionClusterId(cluster.getId());

        // Get envPropertyFile for command and job and set env variable
        if (StringUtils.isNotBlank(command.getEnvPropFile())) {
            this.env.put("COMMAND_ENV_FILE", command.getEnvPropFile());
        }

        if (StringUtils.isNotBlank(this.job.getEnvPropFile())) {
            this.env.put("JOB_ENV_FILE", this.job.getEnvPropFile());
        }

        // put the user name for hadoop to use
        this.env.put("HADOOP_USER_NAME", this.job.getUser());

        // this is for the generic joblauncher.sh to use to create username
        // on the machine if needed
        this.env.put("USER_NAME", this.job.getUser());

        // add the group name
        String groupName = HADOOP_GROUP_NAME;
        if (this.job.getGroup() != null) {
            groupName = this.job.getGroup();
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
            throw new GenieException(
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
                throw new GenieException(
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
                throw new GenieException(
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

        final String copyCommand = hadoopHome + "/bin/hadoop fs " + cpOpts + " -cp -f";
        this.env.put("COPY_COMMAND", copyCommand);

        // Force flag to overwrite required in Hadoop2
        this.env.put("FORCE_COPY_FLAG", "-f");
        
        final String mkdirCommand = hadoopHome + "/bin/hadoop fs " + cpOpts + " -mkdir";
        this.env.put("MKDIR_COMMAND", mkdirCommand);

        // set the base working directory
        final String baseUserWorkingDir = ConfigurationManager
                .getConfigInstance()
                .getString("netflix.genie.server.user.working.dir");
        if (StringUtils.isBlank(baseUserWorkingDir)) {
            final String msg = "Property netflix.genie.server.user.working.dir is not set";
            LOG.error(msg);
            throw new GenieException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
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
    private String convertCollectionToCSV(final Collection<String> list) {
        return StringUtils.join(list, ",");
    }

    /**
     * Extract/initialize command-line arguments passed by user.
     *
     * @return the parsed command-line arguments as an array
     * @throws GenieException
     */
    private String[] initArgs() throws GenieException {
        LOG.info("called");

        String[] cmdArgs = StringUtil.splitCmdLine(this.job.getCommandArgs());

        String[] hArgs;
        hArgs = new String[cmdArgs.length + 2];

        // get the location where genie scripts are installed
        String genieHome = env.get("XS_SYSTEM_HOME");

        // first two args are the joblauncher and job type
        hArgs[0] = genieHome + File.separator + "joblauncher.sh";
        hArgs[1] = this.executable;

        System.arraycopy(cmdArgs, 0, hArgs, 2, cmdArgs.length);

        return hArgs;
    }

//    /**
//     * Return additional command-line arguments specific to genie.
//     *
//     * @return -D style params including genie job id and netflix environment
//     */
//    protected String[] getGenieCmdArgs() {
//        if (this.lipstickUuidProp == null) {
//            return new String[]{
//                "-D",
//                this.genieJobIDProp,
//                "-D",
//                this.netflixEnvProp
//            };
//        } else {
//            return new String[]{
//                "-D",
//                this.genieJobIDProp,
//                "-D",
//                this.netflixEnvProp,
//                "-D",
//                this.lipstickUuidProp
//            };
//        }
//    }

    /**
     * Get process id for the given process.
     *
     * @param proc java process object representing the Hadoop job launcher
     * @return pid for this process
     * @throws GenieException if there is an error getting the process id
     */
    private int getProcessId(final Process proc) throws GenieException {
        LOG.debug("called");

        try {
            final Field f = proc.getClass().getDeclaredField("pid");
            f.setAccessible(true);
            return f.getInt(proc);
        } catch (Exception e) {
            String msg = "Can't get process id for job";
            LOG.error(msg, e);
            throw new GenieException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, msg, e);
        }
    }
}
