package com.netflix.genie.server.jobmanager.impl;

import com.netflix.config.ConfigurationManager;
import com.netflix.genie.common.exceptions.CloudServiceException;
import com.netflix.genie.common.messages.ClusterConfigResponse;
import com.netflix.genie.common.model.ApplicationConfigElement;
import com.netflix.genie.common.model.ClusterConfigElement;
import com.netflix.genie.common.model.CommandConfigElement;
import com.netflix.genie.common.model.FileAttachment;
import com.netflix.genie.common.model.JobElement;
import com.netflix.genie.common.model.Types.JobStatus;
import com.netflix.genie.server.jobmanager.JobManager;
import com.netflix.genie.server.persistence.PersistenceManager;
import com.netflix.genie.server.services.ClusterConfigService;
import com.netflix.genie.server.services.ClusterLoadBalancer;
import com.netflix.genie.server.services.ConfigServiceFactory;
import com.netflix.genie.server.util.StringUtil;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import javax.activation.DataHandler;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of job manager for Yarn Jobs.
 *
 * @author amsharma
 * @author skrishnan
 * @author bmundlapudi
 */
public class YarnJobManager implements JobManager {

    private static final Logger LOG = LoggerFactory
            .getLogger(YarnJobManager.class);

    private final PersistenceManager<CommandConfigElement> pmCommand;

    /**
     * The name of the Genie job id property to be passed to all jobs.
     */
    protected static final String GENIE_JOB_ID = "genie.job.id";

    /**
     * The value of the Genie job id property to pass to jobs.
     */
    protected String genieJobIDProp;

    /**
     * The name of the Environment (test/prod) property to be passed to all
     * jobs.
     */
    protected static final String NFLX_ENV = "netflix.environment";

    /**
     * The value of the environment to be passed to jobs.
     */
    protected String netflixEnvProp;

    /**
     * The value for the Lipstick job ID, if needed.
     */
    protected String lipstickUuidProp;

    /**
     * The environment variables for this job.
     */
    protected Map<String, String> env;

    /**
     * Reference to the cluster load balancer implementation.
     */
    protected ClusterLoadBalancer clb;

    /**
     * Reference to the cluster config element to run the job on.
     */
    protected ClusterConfigElement cluster;

    /**
     * Default group name for job submissions.
     */
    protected static final String HADOOP_GROUP_NAME = "hadoop";

    /**
     * Reference to the cluster config service impl.
     */
    protected ClusterConfigService ccs;

    /**
     * The command-line arguments for this job.
     */
    protected String[] args;

    /**
     * The job info for this job, which is persisted to the database.
     */
    protected JobElement ji;

    /**
     * Actual command to run determined from the CommandConfigElement selected.
     */
    protected String executable;

    /**
     * Default constructor - initializes cluster configuration and load
     * balancer.
     *
     * @throws CloudServiceException if there is any error in initialization
     */
    public YarnJobManager() throws CloudServiceException {
        ccs = ConfigServiceFactory.getClusterConfigImpl();
        clb = ConfigServiceFactory.getClusterLoadBalancer();
        pmCommand = new PersistenceManager<CommandConfigElement>();
    }

    /**
     * Initialize, and launch the job once it has been initialized.
     *
     * @param jInfo the JobInfo object for the job to be launched
     * @throws CloudServiceException if there is any error in the job launch
     */
    @Override
    public void launch(JobElement jInfo) throws CloudServiceException {
        LOG.info("called");

        // initialize all the arguments and environment
        init(jInfo);

        // create the ProcessBuilder for this process
        ProcessBuilder pb = new ProcessBuilder(args);

        // set current working directory for the process
        String cWorkingDir = env.get("BASE_USER_WORKING_DIR") + File.separator
                + ji.getJobID();
        File userJobDir = new File(cWorkingDir);

        // check if working directory already exists
        if (userJobDir.exists()) {
            String msg = "User staging directory already exists";
            ji.setJobStatus(JobStatus.FAILED, msg);
            LOG.error(ji.getStatusMsg() + ": "
                    + userJobDir.getAbsolutePath());
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        }

        // create the working directory
        boolean resMkDir = userJobDir.mkdirs();
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
        if ((ji.getAttachments() != null) && (ji.getAttachments().length > 0)) {
            for (FileAttachment attachment : ji.getAttachments()) {
                // basic error checking
                if ((attachment.getName() == null) || (attachment.getName().isEmpty())) {
                    String msg = "File attachment is missing required parameter name";
                    LOG.error(msg);
                    throw new CloudServiceException(
                            HttpURLConnection.HTTP_BAD_REQUEST, msg);
                }
                if (attachment.getData() == null) {
                    String msg = "File attachment is missing required parameter data";
                    LOG.error(msg);
                    throw new CloudServiceException(
                            HttpURLConnection.HTTP_BAD_REQUEST, msg);
                }
                // good to go - copy attachments
                // not checking for 0-byte attachments - assuming they are legitimate
                FileOutputStream output = null;
                try {
                    output = new FileOutputStream(cWorkingDir + File.separator + attachment.getName());
                    DataHandler inputHandler = attachment.getData();
                    inputHandler.writeTo(output);
                } catch (IOException e) {
                    String msg = "Unable to copy attachment correctly: " + attachment.getName();
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
                            //TODO: Rethrow as CloudServiceException?
                        }
                    }
                }
            }
        }

        // set environment variables for the process
        Map<String, String> penv = pb.environment();
        penv.putAll(env);

        // setup env for current job, conf and jar directories directories
        penv.put("CURRENT_JOB_WORKING_DIR", cWorkingDir);
        penv.put("CURRENT_JOB_CONF_DIR", cWorkingDir + "/conf");
        penv.put("CURRENT_JOB_JAR_DIR", cWorkingDir + "/jars");

        // setup the HADOOP_CONF_DIR
        penv.put("HADOOP_CONF_DIR", cWorkingDir + "/conf");

        int pid;
        try {
            // launch job, and get process handle
            Process proc = pb.start();
            pid = getProcessId(proc);
            ji.setProcessHandle(pid);

            // set off monitor thread for the job
            JobMonitor jobMonitorThread = new JobMonitor(ji, cWorkingDir, proc);
            jobMonitorThread.start();
            ji.setJobStatus(JobStatus.RUNNING, "Job is running");
        } catch (IOException e) {
            String msg = "Failed to launch the job";
            LOG.error(msg, e);
            ji.setJobStatus(JobStatus.FAILED, msg);
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, msg, e);
        }
        LOG.info("Successfully launched the job with PID = " + pid);
    }

    /**
     * Kill the job pointed to by the job info - this only kills the Yarn job
     * shell.
     *
     * @param ji the jobInfo object for the job to be killed
     * @throws CloudServiceException if there is any error in job killing
     */
    @Override
    public void kill(JobElement ji) throws CloudServiceException {
        LOG.info("called");

        // basic error checking
        if (ji == null) {
            String msg = "JobInfo object is null";
            LOG.error(msg);
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        }

        // check to ensure that the process id is actually set (which means job
        // was launched)
        this.ji = ji;
        int processId = ji.getProcessHandle();
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
            } catch (Exception e) {
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
     * @param ji2 the JobInfo object passed by the user
     * @throws CloudServiceException if there is an error during initialization
     */
    protected void init(JobElement ji2) throws CloudServiceException {
        LOG.info("called");

        genieJobIDProp = GENIE_JOB_ID + "=" + ji2.getJobID();
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
        this.env = initEnv(ji2);

        // construct command-line args
        this.args = initArgs(ji2);

        // save/init args, environment and jobinfo
        this.ji = ji2;
    }

    /**
     * Set/initialize environment variables for this job.
     *
     * @param ji2 job info object for this job
     * @return a map containing environment variables for this job
     * @throws CloudServiceException if there is any error in initialization
     */
    protected Map<String, String> initEnv(JobElement ji2) throws CloudServiceException {
        LOG.info("called");

        Map<String, String> hEnv = new HashMap<String, String>();

        if ((ji2.getFileDependencies() != null)
                && (!ji2.getFileDependencies().isEmpty())) {
            hEnv.put("CURRENT_JOB_FILE_DEPENDENCIES", ji2.getFileDependencies());
        }

        // set the hadoop-related conf files
        cluster = getClusterConfig(ji2);
        ArrayList<String> clusterConfigList = cluster.getConfigs();

        hEnv.put("S3_CLUSTER_CONF_FILES", convertListToCSV(clusterConfigList));

        CommandConfigElement command = null;
        ApplicationConfigElement application = null;
        boolean done = false;

        // If Command Id is specified use directly.
        //If command Name is specified iterate through the commands in the cluster to get the Id that matches.
        // To select a command you also have to iterate through the applications if specified to find the one
        // that will be matched to execute the command.
        if ((ji2.getCommandId() != null) && (!(ji2.getCommandId().isEmpty()))) {
            String cmdId = ji2.getCommandId();

            command = pmCommand.getEntity(cmdId, CommandConfigElement.class);
            for (final ApplicationConfigElement ace : command.getApplications()) {
                // If appid is specified check against it. If matches set It and break
                if ((ji2.getApplicationId() != null) && ((!ji2.getApplicationId().isEmpty()))) {
                    if (ace.getId().equals(ji2.getApplicationId())) {
                        application = ace;
                        break;
                    }
                    // If appName is specified check the name. if matches set it and break
                } else if ((ji2.getApplicationName() != null) && ((!ji2.getApplicationName().isEmpty()))) {
                    if (ace.getName().equals(ji2.getApplicationName())) {
                        application = ace;
                        break;
                    }
                    // If appName and appId are not specified use the first app for the command as default value
                } else {
                    //appId = ace.getId();
                    application = ace;
                    break;
                }
            }
        } else if ((ji2.getCommandName() != null) && (!(ji2.getCommandName().isEmpty()))) {
            // Iterate through the commands the cluster supports and find the command that matches.
            // There has to be one that matches, else the getClusterConfig wouldn't have.
            // Check the applications as well
            for (final CommandConfigElement cce : cluster.getCommands()) {
                if (cce.getName().equals(ji2.getCommandName())) {
                    // Name matches. Check Application Details
                    for (final ApplicationConfigElement ace : cce.getApplications()) {
                        // If appid is specified check against it. If matches set It and break
                        if ((ji2.getApplicationId() != null) && ((!ji2.getApplicationId().isEmpty()))) {
                            if (ace.getId().equals(ji2.getApplicationId())) {
                                command = cce;
                                application = ace;
                                done = true;
                                break;
                            }
                            // If appName is specified check the name. if matches set it and break
                        } else if ((ji2.getApplicationName() != null) && ((!ji2.getApplicationName().isEmpty()))) {
                            if (ace.getName().equals(ji2.getApplicationName())) {
                                command = cce;
                                application = ace;
                                done = true;
                                break;
                            }
                            // If appName and appId are not specified use the first app for the command as default value
                        } else {
                            command = cce;
                            application = ace;
                            done = true;
                            break;
                        }
                    }
                    if (done) {
                        break;
                    }
                }
            }
        }

        //Avoiding NPE
        if (command == null) {
            final String msg = "No command found. Unable to continue.";
            LOG.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        }

        // save the command name, application id and application name
        ji2.setCommandId(command.getId());
        ji2.setCommandName(command.getName());

        if (application != null) {
            ji2.setApplicationId(application.getId());
            ji2.setApplicationName(application.getName());
        }

        //CommandConfigElement ce = pmCommand.getEntity(cmdId, CommandConfigElement.class);
        hEnv.put("S3_COMMAND_CONF_FILES", convertListToCSV(command.getConfigs()));
        hEnv.put("S3_APPLICATION_CONF_FILES", convertListToCSV(application.getConfigs()));
        hEnv.put("S3_APPLICATION_JAR_FILES", convertListToCSV(application.getJars()));
        this.executable = command.getExecutable();

        // save the cluster name and id
        ji2.setExecutionClusterName(cluster.getName());
        ji2.setExecutionClusterId(cluster.getId());

        // Get envPropertyFile for application, command and job and read in
        // properties and set it in the environment
        hEnv.put("APPLICATION_ENV_FILE", application.getEnvPropFile());
        hEnv.put("COMMAND_ENV_FILE", command.getEnvPropFile());
        hEnv.put("JOB_ENV_FILE", ji2.getEnvPropFile());

        // put the user name for hadoop to use
        hEnv.put("HADOOP_USER_NAME", ji2.getUserName());

        // this is for the generic joblauncher.sh to use to create username
        // on the machine if needed
        hEnv.put("USER_NAME", ji2.getUserName());

        // add the group name
        String groupName = HADOOP_GROUP_NAME;
        if (ji2.getGroupName() != null) {
            groupName = ji2.getGroupName();
            hEnv.put("GROUP_NAME", groupName);
        }
        hEnv.put("HADOOP_GROUP_NAME", groupName);

        // set the java home
        String javaHome = ConfigurationManager.getConfigInstance().
                getString("netflix.genie.server.java.home");
        if ((javaHome != null) && (!javaHome.isEmpty())) {
            hEnv.put("JAVA_HOME", javaHome);
        }

        // set the genie home
        String genieHome = ConfigurationManager.getConfigInstance()
                .getString("netflix.genie.server.sys.home");
        if ((genieHome == null) || genieHome.isEmpty()) {
            String msg = "Property netflix.genie.server.sys.home is not set correctly";
            LOG.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        }
        hEnv.put("XS_SYSTEM_HOME", genieHome);

        // if the cluster version is provided, overwrite the HADOOP_HOME
        // environment variable
        String hadoopHome;
        if (cluster.getVersion() != null) {
            String hadoopVersion = cluster.getVersion();
            LOG.debug("Hadoop Version of the cluster: " + hadoopVersion);

            // try exact version first
            hadoopHome = ConfigurationManager.getConfigInstance()
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

            if ((hadoopHome == null) || (!new File(hadoopHome).exists())) {
                String msg = "This genie instance doesn't support Hadoop version: "
                        + hadoopVersion;
                LOG.error(msg);
                throw new CloudServiceException(
                        HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
            }

            LOG.info("Overriding HADOOP_HOME from cluster config to: "
                    + hadoopHome);
            hEnv.put("HADOOP_HOME", hadoopHome);
        } else {
            // set the default hadoop home
            hadoopHome = ConfigurationManager.getConfigInstance().
                    getString("netflix.genie.server.hadoop.home");
            if ((hadoopHome == null) || (!new File(hadoopHome).exists())) {
                String msg = "Property netflix.genie.server.hadoop.home is not set correctly";
                LOG.error(msg);
                throw new CloudServiceException(
                        HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
            }
            hEnv.put("HADOOP_HOME", hadoopHome);
        }

        // populate the CP timeout and other options. Yarn jobs would use
        // hadoop fs -cp to copy files. Prepare the copy command with the combination
        // and set the COPY_COMMAND environment variable
        hEnv.put("CP_TIMEOUT",
                ConfigurationManager.getConfigInstance()
                .getString("netflix.genie.server.hadoop.s3cp.timeout", "1800"));

        String cpOpts = "";
        cpOpts = ConfigurationManager.getConfigInstance()
                .getString("netflix.genie.server.hadoop.s3cp.opts", "");

        String copyCommand = hadoopHome + "/bin/hadoop fs " + cpOpts + " -cp";
        hEnv.put("COPY_COMMAND", copyCommand);

        String mkdirCommand = hadoopHome + "/bin/hadoop fs " + cpOpts + " -mkdir";
        hEnv.put("MKDIR_COMMAND", mkdirCommand);

        // set the base working directory
        String baseUserWorkingDir = ConfigurationManager.getConfigInstance().
                getString("netflix.genie.server.user.working.dir");
        if ((baseUserWorkingDir == null) || (baseUserWorkingDir.isEmpty())) {
            String msg = "Property netflix.genie.server.user.working.dir is not set";
            LOG.error(msg);
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        } else {
            hEnv.put("BASE_USER_WORKING_DIR", baseUserWorkingDir);
        }

        // set the archive location
        // unless user has explicitly requested for it to be disabled
        if (!ji2.isDisableLogArchival()) {
            String s3ArchiveLocation = ConfigurationManager.getConfigInstance()
                    .getString("netflix.genie.server.s3.archive.location");
            if ((s3ArchiveLocation != null) && (!s3ArchiveLocation.isEmpty())) {
                hEnv.put("S3_ARCHIVE_LOCATION", s3ArchiveLocation);
            }
        }

        // set the variables to be added to the core-site xml. Format of this variable is:
        // key1=value1;key2=value2;key3=value3
        hEnv.put("CORE_SITE_XML_ARGS", genieJobIDProp + ";" + netflixEnvProp + ";" + lipstickUuidProp);

        return hEnv;
    }

    /**
     * Converts a list of strings to a csv.
     *
     * @param list ArrayList object contains the strings
     * @return a string containing the other strings as csv
     */
    protected String convertListToCSV(final ArrayList<String> list) {
        return StringUtils.join(list, ",");
    }

    /**
     * Figure out an appropriate cluster to run this job<br>
     * Cluster selection is done based on tags, command and application.
     *
     * @param ji2 job info for this job
     * @return cluster config element to use for running this job
     * @throws CloudServiceException if there is any error finding a cluster for
     * this job
     */
    protected ClusterConfigElement getClusterConfig(JobElement ji2)
            throws CloudServiceException {
        LOG.info("called");

        ClusterConfigResponse ccr = null;

        ccr = ccs.getClusterConfig(ji2.getApplicationId(),
                ji2.getApplicationName(),
                ji2.getCommandId(),
                ji2.getCommandName(),
                ji2.getClusterCriteriaList());

        // return selected instance
        return clb.selectCluster(ccr.getClusterConfigs());
    }

    /**
     * Extract/initialize command-line arguments passed by user.
     *
     * @param ji2 job info for this job
     * @return the parsed command-line arguments as an array
     * @throws CloudServiceException
     */
    protected String[] initArgs(JobElement ji2) throws CloudServiceException {

        LOG.info("called");

        String[] cmdArgs = StringUtil.splitCmdLine(ji2.getCmdArgs());

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
        if (lipstickUuidProp == null) {
            return new String[]{"-D", genieJobIDProp, "-D", netflixEnvProp};
        } else {
            return new String[]{"-D", genieJobIDProp, "-D", netflixEnvProp, "-D", lipstickUuidProp};
        }
    }

    /**
     * Get process id for the given process.
     *
     * @param proc java process object representing the Hadoop job launcher
     * @return pid for this process
     * @throws CloudServiceException if there is an error getting the process id
     */
    protected int getProcessId(Process proc) throws CloudServiceException {
        LOG.debug("called");

        try {
            Field f = proc.getClass().getDeclaredField("pid");
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
