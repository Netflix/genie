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

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.HttpURLConnection;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.config.ConfigurationManager;
import com.netflix.genie.common.exceptions.CloudServiceException;
import com.netflix.genie.common.messages.ClusterConfigResponse;
import com.netflix.genie.common.model.ClusterConfigElement;
import com.netflix.genie.common.model.JobInfoElement;
import com.netflix.genie.common.model.Types;
import com.netflix.genie.common.model.Types.JobStatus;
import com.netflix.genie.server.jobmanager.JobManager;
import com.netflix.genie.server.services.ClusterConfigService;
import com.netflix.genie.server.services.ClusterLoadBalancer;
import com.netflix.genie.server.services.ConfigServiceFactory;
import com.netflix.genie.server.util.StringUtil;

/**
 * Implementation of job manager for vanilla Hadoop jobs.
 *
 * @author skrishnan
 * @author bmundlapudi
 */
public class HadoopJobManager implements JobManager {

    private static Logger logger = LoggerFactory
            .getLogger(HadoopJobManager.class);

    /**
     * Reference to the cluster config service impl.
     */
    protected ClusterConfigService ccs;
    /**
     * Reference to the cluster config element to run the job on.
     */
    protected ClusterConfigElement cluster;
    /**
     * Reference to the cluster load balancer implementation.
     */
    protected ClusterLoadBalancer clb;

    /**
     * Default group name for job submissions.
     */
    protected static final String HADOOP_GROUP_NAME = "hadoop";

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
     * The job info for this job, which is persisted to the database.
     */
    protected JobInfoElement ji;

    /**
     * The command-line arguments for this job.
     */
    protected String[] args;

    /**
     * The environment variables for this job.
     */
    protected Map<String, String> env;

    /**
     * Default constructor - initializes cluster configuration and load
     * balancer.
     *
     * @throws CloudServiceException
     *             if there is any error in initialization
     */
    public HadoopJobManager() throws CloudServiceException {
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
    public void launch(JobInfoElement ji) throws CloudServiceException {
        logger.info("called");

        // initialize all the arguments and environment
        init(ji);

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
     * Hadoop/Hive/Pig shell.
     *
     * @param ji
     *            the jobInfo object for the job to be killed
     * @throws CloudServiceException
     *             if there is any error in job killing
     */
    @Override
    public void kill(JobInfoElement ji) throws CloudServiceException {
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

    /**
     * Initializes the object with the job information and environment prior to
     * job launch This method must be called before job is launched.
     *
     * @param ji
     *            the JobInfo object passed by the user
     * @throws CloudServiceException
     *             if there is an error during initialization
     */
    protected void init(JobInfoElement ji) throws CloudServiceException {
        logger.info("called");

        genieJobIDProp = GENIE_JOB_ID + "=" + ji.getJobID();
        netflixEnvProp = NFLX_ENV
                + "="
                + ConfigurationManager.getConfigInstance().getString(
                        "netflix.environment");

        // construct the environment variables
        this.env = initEnv(ji);

        // construct command-line args
        this.args = initArgs(ji);

        // save/init args, environment and jobinfo
        this.ji = ji;
    }

    /**
     * Return additional command-line arguments specific to genie.
     *
     * @return -D style params including genie job id and netflix environment
     */
    protected String[] getGenieCmdArgs() {
        return new String[] {"-D", genieJobIDProp, "-D", netflixEnvProp};
    }

    /**
     * Extract/initialize command-line arguments passed by user.
     *
     * @param ji
     *            job info for this job
     * @return the parsed command-line arguments as an array
     * @throws CloudServiceException
     */
    protected String[] initArgs(JobInfoElement ji) throws CloudServiceException {
        logger.info("called");

        String[] cmdArgs = StringUtil.splitCmdLine(ji.getCmdArgs());
        String[] genieArgs = getGenieCmdArgs();

        // 2 additional args for joblauncher and job type
        String[] hArgs;
        if (Types.JobType.parse(ji.getJobType()) == Types.JobType.HADOOP) {
            // no generic way to do this for Hadoop since CLASS is optional
            // so don't pass genie job id for now
            hArgs = new String[cmdArgs.length + 2];
        } else {
            hArgs = new String[cmdArgs.length + genieArgs.length + 2];
        }

        // get the location where genie scripts are installed
        String genieHome = env.get("XS_SYSTEM_HOME");

        // first two args are the joblauncher and job type
        hArgs[0] = genieHome + File.separator + "joblauncher.sh";
        hArgs[1] = ji.getJobType().toLowerCase();

        // incorporate the genieArgs into the command-line for each case
        if (Types.JobType.parse(ji.getJobType()) == Types.JobType.HADOOP) {
            logger.info("Not prepending genieArgs for hadoop job for now");
            System.arraycopy(cmdArgs, 0, hArgs, 2, cmdArgs.length);
        } else {
            logger.info("Prepending genieArgs to link genie job to hadoop jobs");

            // prepend before user args
            System.arraycopy(genieArgs, 0, hArgs, 2, genieArgs.length);

            // now copy over the rest of the user args
            System.arraycopy(cmdArgs, 0, hArgs, 2 + genieArgs.length,
                    cmdArgs.length);
        }

        return hArgs;
    }

    /**
     * Set/initialize environment variables for this job.
     *
     * @param ji
     *            job info object for this job
     * @return a map containing environment variables for this job
     * @throws CloudServiceException
     *             if there is any error in initialization
     */
    protected Map<String, String> initEnv(JobInfoElement ji)
            throws CloudServiceException {
        logger.info("called");

        Map<String, String> hEnv = new HashMap<String, String>();

        if ((ji.getFileDependencies() != null)
                && (!ji.getFileDependencies().isEmpty())) {
            hEnv.put("CURRENT_JOB_FILE_DEPENDENCIES", ji.getFileDependencies());
        }

        // set the hadoop-related conf files
        cluster = getClusterConfig(ji);
        String s3HadoopConfLocation = cluster.getS3SiteXmlsAsCsv();
        hEnv.put("S3_HADOOP_CONF_FILES", s3HadoopConfLocation);

        // save the cluster name and id
        ji.setClusterName(cluster.getName());
        ji.setClusterId(cluster.getId());

        // put the user name for hadoop to use
        hEnv.put("HADOOP_USER_NAME", ji.getUserName());

        // add the group name
        String groupName = HADOOP_GROUP_NAME;
        if (ji.getGroupName() != null) {
            groupName = ji.getGroupName();
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

        // if the cluster version is provided, overwrite the HADOOP_HOME
        // environment variable
        if (cluster.getHadoopVersion() != null) {
            String hadoopVersion = cluster.getHadoopVersion();

            // try exact version first
            String hadoopHome = ConfigurationManager.getConfigInstance()
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
                logger.error(msg);
                throw new CloudServiceException(
                        HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
            }

            logger.info("Overriding HADOOP_HOME from cluster config to: "
                    + hadoopHome);
            hEnv.put("HADOOP_HOME", hadoopHome);
        } else {
            // set the default hadoop home
            String hadoopHome = ConfigurationManager.getConfigInstance().
                    getString("netflix.genie.server.hadoop.home");
            if ((hadoopHome == null) || (!new File(hadoopHome).exists())) {
                String msg = "Property netflix.genie.server.hadoop.home is not set correctly";
                logger.error(msg);
                throw new CloudServiceException(
                        HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
            }
            hEnv.put("HADOOP_HOME", hadoopHome);
        }

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
        String s3ArchiveLocation = ConfigurationManager.getConfigInstance()
                .getString("netflix.genie.server.s3.archive.location");
        if ((s3ArchiveLocation != null) && (!s3ArchiveLocation.isEmpty())) {
            hEnv.put("S3_ARCHIVE_LOCATION", s3ArchiveLocation);
        }

        return hEnv;
    }

    /**
     * Figure out an appropriate cluster to run this job<br>
     * Schedule is ignored if clusterId or clusterName are not null.
     *
     * @param ji
     *            job info for this job
     * @return cluster config element to use for running this job
     * @throws CloudServiceException
     *             if there is any error finding a cluster for this job
     */
    protected ClusterConfigElement getClusterConfig(JobInfoElement ji)
            throws CloudServiceException {
        logger.info("called");

        ClusterConfigResponse ccr;
        String clusterId = ji.getClusterId();
        String clusterName = ji.getClusterName();
        String schedule = null;
        // only use the schedule if both cluster id and cluster name are null
        if ((clusterId == null) && (clusterName == null)) {
            schedule = ji.getSchedule();
        }

        ccr = ccs.getClusterConfig(clusterId, clusterName,
                Types.Configuration.parse(ji.getConfiguration()),
                Types.Schedule.parse(schedule),
                Types.JobType.parse(ji.getJobType()), Types.ClusterStatus.UP);

        // return selected instance
        return clb.selectCluster(ccr.getClusterConfigs());
    }
}
