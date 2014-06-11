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
package com.netflix.genie.server.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.appinfo.InstanceInfo;
import com.netflix.appinfo.InstanceInfo.InstanceStatus;
import com.netflix.discovery.DiscoveryClient;
import com.netflix.discovery.DiscoveryManager;
import com.netflix.discovery.shared.Application;
import com.netflix.genie.common.exceptions.CloudServiceException;
import com.netflix.genie.common.model.Job;
import com.netflix.genie.common.model.Job_;
import com.netflix.genie.common.model.Types.JobStatus;
import com.netflix.genie.server.persistence.PersistenceManager;
import com.netflix.genie.server.util.NetUtil;
import java.util.ArrayList;
import java.util.List;
import javax.persistence.EntityManager;
import javax.persistence.TypedQuery;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;

/**
 * Utility class to get number of jobs running on this instance.
 *
 * @author skrishnan
 * @author tgianos
 */
public final class JobCountManager {

    private static final Logger LOG = LoggerFactory
            .getLogger(JobCountManager.class);

    private static volatile PersistenceManager<Job> pm;

    /**
     * Private constructor - never called.
     */
    private JobCountManager() {
    }

    /**
     * Use this method to initialize prior to use.
     */
    private static synchronized void init() {
        LOG.info("called");
        if (pm == null) {
            pm = new PersistenceManager<Job>();
        }
    }

    /**
     * Get number of running jobs running on this instance.
     *
     * @return number of running jobs on this instance
     * @throws CloudServiceException if there is an error
     */
    public static int getNumInstanceJobs() throws CloudServiceException {
        LOG.debug("called");

        return getNumInstanceJobs(null, null, null);
    }

    /**
     * Get number of running jobs with minStartTime &lt; startTime &gt;
     * maxStartTime on this instance min/max startTimes are ignored if they are
     * null.
     *
     * @param minStartTime min start time in ms
     * @param maxStartTime max start time in ms
     * @return number of running jobs between the specified times
     * @throws CloudServiceException if there is an error
     */
    public static int getNumInstanceJobs(
            final Long minStartTime,
            final Long maxStartTime)
            throws CloudServiceException {
        LOG.debug("called");

        return getNumInstanceJobs(null, minStartTime, maxStartTime);
    }

    /**
     * Get number of running jobs with minStartTime <= startTime < maxStartTime
     * min/max startTimes are ignored if they are null.
     *
     * @param hostName - if null, local host name is used
     * @param minStartTime min start time in ms
     * @param maxStartTime max start time in ms
     * @return number of running jobs matching specified critiera
     * @throws CloudServiceException if there is an error
     */
    public static int getNumInstanceJobs(
            String hostName,
            final Long minStartTime,
            final Long maxStartTime) throws CloudServiceException {
        LOG.debug("called");

        // initialize host name
        if (hostName == null) {
            hostName = NetUtil.getHostName();
        }
        if (pm == null) {
            init();
        }
        final EntityManager em = pm.createEntityManager();
        try {
            final CriteriaBuilder cb = em.getCriteriaBuilder();
            final CriteriaQuery<Long> cq = cb.createQuery(Long.class);
            final Root<Job> j = cq.from(Job.class);
            cq.select(cb.count(j));
//            final SingularAttribute<Job, JobStatus> attr = Job_.status;
//            Path<JobStatus> status = j.get(Job_.status);
            final Predicate runningStatus = cb.equal(j.get(Job_.status), JobStatus.RUNNING);
            final Predicate initStatus = cb.equal(j.get(Job_.status), JobStatus.INIT);
            final List<Predicate> predicates = new ArrayList<Predicate>();
            predicates.add(cb.equal(j.get(Job_.hostName), hostName));
            predicates.add(cb.or(runningStatus, initStatus));
            if (minStartTime != null) {
                predicates.add(cb.ge(j.get(Job_.startTime), minStartTime));
            }
            if (maxStartTime != null) {
                predicates.add(cb.lt(j.get(Job_.startTime), maxStartTime));
            }
            //documentation says that by default predicate array is conjuncted together
            cq.where(predicates.toArray(new Predicate[0]));
            final TypedQuery<Long> query = em.createQuery(cq);
            //Downgrading to an int since all the code seems to want ints
            //Don't feel like changing everthing right now can figure out later
            //if need be
            return query.getSingleResult().intValue();
        } finally {
            em.close();
        }
    }

    /**
     * Returns the most idle Genie instance (&lt; minJobThreshold running jobs),
     * if possible - else returns current instance.
     *
     * @param minJobThreshold the threshold to use to look for idle instances
     * @return host name of most idle Genie instance
     * @throws CloudServiceException if there is any error
     */
    public static String getIdleInstance(
            final long minJobThreshold)
            throws CloudServiceException {
        LOG.debug("called");
        String localhost = NetUtil.getHostName();

        // naive implementation where we loop through all instances in discovery
        // no need to raise any exceptions here, just return localhost if there
        // is any error
        DiscoveryClient discoveryClient = DiscoveryManager.getInstance()
                .getDiscoveryClient();
        if (discoveryClient == null) {
            LOG.warn("Can't instantiate DiscoveryClient - returning localhost");
            return localhost;
        }
        Application app = discoveryClient.getApplication("genie");
        if (app == null) {
            LOG.warn("Discovery client can't find genie - returning localhost");
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
            LOG.debug("Trying host name: " + hostName);
            final int numInstanceJobs = getNumInstanceJobs(hostName, null, null);
            if (numInstanceJobs <= minJobThreshold) {
                LOG.info("Returning idle host: " + hostName + ", who has "
                        + numInstanceJobs + " jobs running");
                return hostName;
            } else {
                LOG.debug("Host: " + hostName + " skipped since it has "
                        + numInstanceJobs + " running jobs, threshold is: "
                        + minJobThreshold);
            }
        }

        // no hosts found with numInstanceJobs < minJobThreshold, return current
        // instance
        LOG.info("Can't find any host to foward to - returning localhost");
        return localhost;
    }
}
