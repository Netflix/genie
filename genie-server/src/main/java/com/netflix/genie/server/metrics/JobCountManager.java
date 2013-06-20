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

package com.netflix.genie.server.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.appinfo.InstanceInfo;
import com.netflix.appinfo.InstanceInfo.InstanceStatus;
import com.netflix.discovery.DiscoveryClient;
import com.netflix.discovery.DiscoveryManager;
import com.netflix.discovery.shared.Application;
import com.netflix.genie.common.exceptions.CloudServiceException;
import com.netflix.genie.common.model.JobInfoElement;
import com.netflix.genie.server.persistence.ClauseBuilder;
import com.netflix.genie.server.persistence.PersistenceManager;
import com.netflix.genie.server.persistence.QueryBuilder;
import com.netflix.genie.server.util.NetUtil;

/**
 * Utility class to get number of jobs running on this instance.
 *
 * @author skrishnan
 */
public final class JobCountManager {

    private static Logger logger = LoggerFactory
            .getLogger(JobCountManager.class);

    private static volatile PersistenceManager<JobInfoElement> pm;

    /**
     * Private constructor - never called.
     */
    private JobCountManager() {
    }

    /**
     * Use this method to initialize prior to use.
     */
    public static synchronized void init() {
        logger.info("called");
        if (pm == null) {
            pm = new PersistenceManager<JobInfoElement>();
        }
    }

    /**
     * Get number of running jobs running on this instance.
     *
     * @return number of running jobs on this instance
     * @throws CloudServiceException
     *             if there is an error
     */
    public static int getNumInstanceJobs() throws CloudServiceException {
        logger.debug("called");

        return getNumInstanceJobs(null, null, null);
    }

    /**
     * Get number of running jobs with minStartTime &lt; startTime &gt;
     * maxStartTime on this instance min/max startTimes are ignored if they are
     * null.
     *
     * @param minStartTime
     *            min start time in ms
     * @param maxStartTime
     *            max start time in ms
     * @return number of running jobs between the specified times
     * @throws CloudServiceException
     *             if there is an error
     */
    public static int getNumInstanceJobs(Long minStartTime, Long maxStartTime)
            throws CloudServiceException {
        logger.debug("called");

        return getNumInstanceJobs(null, minStartTime, maxStartTime);
    }

    /**
     * Get number of running jobs with minStartTime < startTime < maxStartTime
     * min/max startTimes are ignored if they are null.
     *
     * @param hostName
     *            - if null, local host name is used
     * @param minStartTime
     *            min start time in ms
     * @param maxStartTime
     *            max start time in ms
     * @return number of running jobs matching specified critiera
     * @throws CloudServiceException
     *             if there is an error
     */
    public static int getNumInstanceJobs(String hostName, Long minStartTime,
            Long maxStartTime) throws CloudServiceException {
        logger.debug("called");

        // initialize host name
        if (hostName == null) {
            hostName = NetUtil.getHostName();
        }

        // return number of jobs running/init-ed on this instance
        ClauseBuilder criteria = new ClauseBuilder(ClauseBuilder.AND);
        criteria.append("hostName = '" + hostName + "'");
        if (minStartTime != null) {
            criteria.append("startTime > " + minStartTime);
        }
        if (maxStartTime != null) {
            criteria.append("startTime < " + maxStartTime);
        }
        ClauseBuilder status = new ClauseBuilder(ClauseBuilder.OR);
        status.append("status='RUNNING'");
        status.append("status='INIT'");
        criteria.append("(" + status.toString() + ")", false);

        // initialize, if need be
        if (pm == null) {
            init();
        }

        // query without pagination
        QueryBuilder builder = new QueryBuilder().table("JobInfoElement")
                .clause(criteria.toString()).paginate(false);
        Object[] results = pm.query(builder);
        if (results == null) {
            return 0;
        } else {
            return results.length;
        }
    }

    /**
     * Returns the most idle Genie instance (&lt; minJobThreshold running jobs),
     * if possible - else returns current instance.
     *
     * @param minJobThreshold
     *            the threshold to use to look for idle instances
     * @return host name of most idle Genie instance
     * @throws CloudServiceException
     *             if there is any error
     */
    public static String getIdleInstance(int minJobThreshold)
            throws CloudServiceException {
        logger.debug("called");
        String localhost = NetUtil.getHostName();

        // naive implementation where we loop through all instances in discovery
        // no need to raise any exceptions here, just return localhost if there
        // is any error
        DiscoveryClient discoveryClient = DiscoveryManager.getInstance()
                .getDiscoveryClient();
        if (discoveryClient == null) {
            logger.warn("Can't instantiate DiscoveryClient - returning localhost");
            return localhost;
        }
        Application app = discoveryClient.getApplication("genie");
        if (app == null) {
            logger.warn("Discovery client can't find genie - returning localhost");
            return localhost;
        }

        for (InstanceInfo instance : app.getInstances()) {
            // only pick instances that are UP
            if ((instance.getStatus() == null)
                    || (instance.getStatus() != InstanceStatus.UP)) {
                continue;
            }

            // if instance is UP, check if job can be forwarded to it
            String hostName = instance.getHostName();
            logger.debug("Trying host name: " + hostName);
            int numInstanceJobs = getNumInstanceJobs(hostName, null, null);
            if (numInstanceJobs <= minJobThreshold) {
                logger.info("Returning idle host: " + hostName + ", who has "
                        + numInstanceJobs + " jobs running");
                return hostName;
            } else {
                logger.debug("Host: " + hostName + " skipped since it has "
                        + numInstanceJobs + " running jobs, threshold is: "
                        + minJobThreshold);
            }
        }

        // no hosts found with numInstanceJobs < minJobThreshold, return current
        // instance
        logger.info("Can't find any host to foward to - returning localhost");
        return localhost;
    }
}
