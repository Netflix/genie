package com.netflix.genie.server.jobmanager.impl;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.activation.DataHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.config.ConfigurationManager;
import com.netflix.genie.common.exceptions.CloudServiceException;
import com.netflix.genie.common.messages.ClusterConfigResponse;
import com.netflix.genie.common.messages.ClusterConfigResponseOld;
import com.netflix.genie.common.model.ClusterConfigElement;
import com.netflix.genie.common.model.ClusterConfigElementOld;
import com.netflix.genie.common.model.FileAttachment;
import com.netflix.genie.common.model.JobElement;
import com.netflix.genie.common.model.Types.JobStatus;
import com.netflix.genie.server.jobmanager.JobManager;
import com.netflix.genie.server.services.ClusterConfigService;
import com.netflix.genie.server.services.ClusterLoadBalancer;
import com.netflix.genie.server.services.ConfigServiceFactory;
import com.netflix.genie.server.util.StringUtil;

/**
 *  Implementation of job manager for Yarn Jobs 
 * @author amsharma
 * @author skrishnan
 * @author bmundlapudi
 */
public class YarnJobManager implements JobManager {
    
    private static Logger logger = LoggerFactory
            .getLogger(YarnJobManager.class);

    /**
     * The name of the Genie job id property to be passed to all jobs.
     */
    protected static final String GENIE_JOB_ID = "genie.job.id";

    /**
     * The value of the Genie job id property to pass to jobs.
     */
    protected String genieJobIDProp;

    /**
     * The name of the Environment (test/prod) property to be passed to all jobs.
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
     * Default constructor - initializes cluster configuration and load
     * balancer.
     *
     * @throws CloudServiceException
     *             if there is any error in initialization
     */
    public YarnJobManager() throws CloudServiceException {
        ccs = ConfigServiceFactory.getClusterConfigImpl();
        clb = ConfigServiceFactory.getClusterLoadBalancer();
    }
    
    /**
     * Initialize, and launch the job once it has been initialized.
     *
     * @param ji
     *            the JobInfo object for the job to be launched
     * @throws CloudServiceException
     *             if there is any error in the job launch
     */
    @Override
    public void launch(JobElement jInfo) throws CloudServiceException {
        logger.info("called");
        
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
            logger.error(ji.getStatusMsg() + ": "
                    + userJobDir.getAbsolutePath());
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        }

        // create the working directory
        boolean resMkDir = userJobDir.mkdirs();
        if (!resMkDir) {
            String msg = "User staging directory can't be created";
            ji.setJobStatus(JobStatus.FAILED, msg);
            logger.error(ji.getStatusMsg() + ": "
                    + userJobDir.getAbsolutePath());
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        }
        pb.directory(userJobDir);
        
        // copy over the attachments if they exist
        if ((ji.getAttachments() != null) && (ji.getAttachments().length > 0)) {
            for (int i = 0; i < ji.getAttachments().length; i++) {
                FileAttachment attachment = ji.getAttachments()[i];
                // basic error checking
                if ((attachment.getName() == null) || (attachment.getName().isEmpty())) {
                    String msg = "File attachment is missing required parameter name";
                    logger.error(msg);
                    throw new CloudServiceException(
                            HttpURLConnection.HTTP_BAD_REQUEST, msg);
                }
                if (attachment.getData() == null) {
                    String msg = "File attachment is missing required parameter data";
                    logger.error(msg);
                    throw new CloudServiceException(
                            HttpURLConnection.HTTP_BAD_REQUEST, msg);
                }
                // good to go - copy attachments
                // not checking for 0-byte attachments - assuming they are legitimate
                try {
                    FileOutputStream output = new FileOutputStream(cWorkingDir + File.separator + attachment.getName());
                    DataHandler inputHandler = attachment.getData();
                    inputHandler.writeTo(output);
                    output.close();
                } catch (IOException e) {
                    String msg = "Unable to copy attachment correctly: " + attachment.getName();
                    logger.error(msg);
                    throw new CloudServiceException(
                            HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
                }
            }
        }
        
        // set environment variables for the process
        Map<String, String> penv = pb.environment();
        penv.putAll(env);

        // setup env for current job directories
        penv.put("CURRENT_JOB_WORKING_DIR", cWorkingDir);
        penv.put("CURRENT_JOB_CONF_DIR", cWorkingDir + "/conf");

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
            logger.error(msg, e);
            ji.setJobStatus(JobStatus.FAILED, msg);
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, msg, e);
        }
        logger.info("Successfully launched the job with PID = " + pid);
    }

    /**
     * Kill the job pointed to by the job info - this only kills the
     * Yarn job shell.
     *
     * @param ji
     *            the jobInfo object for the job to be killed
     * @throws CloudServiceException
     *             if there is any error in job killing
     */
    @Override
    public void kill(JobElement ji) throws CloudServiceException {
        logger.info("called");

        // basic error checking
        if (ji == null) {
            String msg = "JobInfo object is null";
            logger.error(msg);
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        }

        // check to ensure that the process id is actually set (which means job
        // was launched)
        this.ji = ji;
        int processId = ji.getProcessHandle();
        if (processId > 0) {
            logger.info("Attempting to kill the process " + processId);
            try {
                String genieHome = ConfigurationManager.getConfigInstance()
                        .getString("netflix.genie.server.sys.home");
                if ((genieHome == null) || genieHome.isEmpty()) {
                    String msg = "Property netflix.genie.server.sys.home is not set correctly";
                    logger.error(msg);
                    throw new CloudServiceException(HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
                }
                Runtime.getRuntime().exec(
                        genieHome + File.separator + "jobkill.sh " + processId);
            } catch (Exception e) {
                String msg = "Failed to kill the job";
                logger.error(msg, e);
                throw new CloudServiceException(
                        HttpURLConnection.HTTP_INTERNAL_ERROR, msg, e);
            }
        } else {
            String msg = "Could not get process id";
            logger.error(msg);
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        }
    }
    
    /**
     * Initializes the object with the job information and environment prior to
     * job launch This method must be called before job is launched.
     *
     * @param jInfo
     *            the JobInfo object passed by the user
     * @throws CloudServiceException
     *             if there is an error during initialization
     */
    protected void init(JobElement ji2) throws CloudServiceException {
        logger.info("called");
        
        genieJobIDProp = GENIE_JOB_ID + "=" + ji2.getJobID();
        netflixEnvProp = NFLX_ENV + "="
                + ConfigurationManager.getConfigInstance().getString(
                        "netflix.environment");

        String lipstickUuidPropName = ConfigurationManager.getConfigInstance().
                getString("netflix.genie.server.lipstick.uuid.prop.name", "lipstick.uuid.prop.name");

        // #TODO i am not sure lipstick UUID setting belongs in open source version of genie. 
        // Maybe think of some other way of setting it
        // set the lipstick job ID, if needed
        if (ConfigurationManager.getConfigInstance().getBoolean(
                        "netflix.genie.server.lipstick.enable", false)) {
            lipstickUuidProp = lipstickUuidPropName + "=" + GENIE_JOB_ID;
        }
        
        // construct the environment variables
        this.env = initEnv(ji2);
        
        // construct command-line args
        this.args = initArgs(ji2);
        
        // save/init args, environment and jobinfo
        // TODO do we need to do this ... aaargghh... java reference/value confusion again
        this.ji = ji2;
    }



    /**
     * Set/initialize environment variables for this job.
     *
     * @param ji2
     *            job info object for this job
     * @return a map containing environment variables for this job
     * @throws CloudServiceException
     *             if there is any error in initialization
     */
    protected Map<String, String> initEnv(JobElement ji2) throws CloudServiceException {
        logger.info("called");
        
        Map<String, String> hEnv = new HashMap<String, String>();

        if ((ji2.getFileDependencies() != null)
                && (!ji2.getFileDependencies().isEmpty())) {
            hEnv.put("CURRENT_JOB_FILE_DEPENDENCIES", ji2.getFileDependencies());
        }
        
        // set the hadoop-related conf files
        cluster = getClusterConfig(ji2);
        //String s3HadoopConfLocation = cluster.getS3SiteXmlsAsCsv();
        //hEnv.put("S3_HADOOP_CONF_FILES", s3HadoopConfLocation);
        ArrayList<String> clusterConfigList = cluster.getConfigs();
        
        
        hEnv.put("S3_CLUSTER_CONF_FILES", convertListToCSV(clusterConfigList)); 
        
        // save the cluster name and id
        ji2.setExecutionClusterName(cluster.getName());
        ji2.setExecutionClusterId(cluster.getId());
        
        //TODO: Set env variables to download command and application configs and jars
        
        // put the user name for hadoop to use
        hEnv.put("HADOOP_USER_NAME", ji2.getUserName());
        
        // add the group name
        String groupName = HADOOP_GROUP_NAME;
        if (ji2.getGroupName() != null) {
            groupName = ji2.getGroupName();
        }
        hEnv.put("HADOOP_GROUP_NAME", groupName);
        
        // put the s3cp parameters
        hEnv.put("HADOOP_S3CP_TIMEOUT",
                ConfigurationManager.getConfigInstance()
                .getString("netflix.genie.server.hadoop.s3cp.timeout", "1800"));
        hEnv.put("HADOOP_S3CP_OPTS",
                ConfigurationManager.getConfigInstance()
                .getString("netflix.genie.server.hadoop.s3cp.opts", ""));

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
            logger.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        }
        hEnv.put("XS_SYSTEM_HOME", genieHome);
        
        // TODO: logic to set HADOOP_HOME
        // This should actually be specified in the cluster env or commands? we just need to check if genie supports it
        
        // set the base working directory
        String baseUserWorkingDir = ConfigurationManager.getConfigInstance().
                getString("netflix.genie.server.user.working.dir");
        if ((baseUserWorkingDir == null) || (baseUserWorkingDir.isEmpty())) {
            String msg = "Property netflix.genie.server.user.working.dir is not set";
            logger.error(msg);
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
        
        return hEnv;
    }

    /**
     * Converts a list of strings to a csv
     *
     * @param list
     *            ArrayList object contains the strings
     * @return a string containing the other strings as csv
     */
    protected String convertListToCSV(ArrayList<String> list) {
        Iterator<String> it = list.iterator();
        String csv = new String();
        
        while(it.hasNext()) {
            csv.concat((String)it.next()+",");
        }
        
        // Trim the last comma
        csv.substring(0, csv.length()-1);
        return csv;
    }

    /**
     * Figure out an appropriate cluster to run this job<br>
     * Cluster selection is done based on tags, command and application
     *
     * @param ji2
     *            job info for this job
     * @return cluster config element to use for running this job
     * @throws CloudServiceException
     *             if there is any error finding a cluster for this job
     */
    protected ClusterConfigElement getClusterConfig(JobElement ji2)
            throws CloudServiceException {
        logger.info("called");

          ClusterConfigResponse ccr = null;
          
          // TODO Logic to determine cluster based on command/application combination
          
          ClusterConfigElement ce = new ClusterConfigElement();
//        String clusterId = ji2.getClusterId();
//        String clusterName = ji2.getClusterName();
//        String schedule = null;
//        // only use the schedule if both cluster id and cluster name are null or empty
//        if (((clusterId == null) || clusterId.isEmpty())
//                && ((clusterName == null) || clusterName.isEmpty())) {
//            schedule = ji2.getSchedule();
//        }
//
//        ccr = ccs.getClusterConfig(clusterId, clusterName,
//                Types.Configuration.parse(ji2.getConfiguration()),
//                Types.Schedule.parse(schedule),
//                Types.JobType.parse(ji2.getJobType()), Types.ClusterStatus.UP);
//
//        // return selected instance
        //return clb.selectCluster(ccr.getClusterConfigs());
          return ce;
    }
    
    /**
     * Extract/initialize command-line arguments passed by user.
     *
     * @param ji2
     *            job info for this job
     * @return the parsed command-line arguments as an array
     * @throws CloudServiceException
     */
    protected String[] initArgs(JobElement ji2) throws CloudServiceException {
        
        logger.info("called");

        String[] cmdArgs = StringUtil.splitCmdLine(ji2.getCmdArgs());
        String[] genieArgs = getGenieCmdArgs();
        
        // TODO Figure out length of hArgs based on args
        String[] hArgs;
        hArgs = new String[10];
        
        // TODO construct the hargs based on command
        return hArgs;
    }
    
    /**
     * Return additional command-line arguments specific to genie.
     *
     * @return -D style params including genie job id and netflix environment
     */
    protected String[] getGenieCmdArgs() {
        if (lipstickUuidProp == null) {
            return new String[] {"-D", genieJobIDProp, "-D", netflixEnvProp};
        } else {
            return new String[] {"-D", genieJobIDProp, "-D", netflixEnvProp, "-D", lipstickUuidProp};
        }
    }
    

    /**
     * Get process id for the given process.
     *
     * @param proc
     *            java process object representing the Hadoop job launcher
     * @return pid for this process
     * @throws CloudServiceException
     *             if there is an error getting the process id
     */
    protected int getProcessId(Process proc) throws CloudServiceException {
        logger.debug("called");

        try {
            Field f = proc.getClass().getDeclaredField("pid");
            f.setAccessible(true);
            return f.getInt(proc);
        } catch (Exception e) {
            String msg = "Can't get process id for job";
            logger.error(msg, e);
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, msg, e);
        }
    }
}
