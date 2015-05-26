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
package com.netflix.genie.server.metrics.impl;

import com.netflix.appinfo.InstanceInfo;
import com.netflix.appinfo.InstanceInfo.InstanceStatus;
import com.netflix.config.ConfigurationManager;
import com.netflix.discovery.DiscoveryClient;
import com.netflix.discovery.DiscoveryManager;
import com.netflix.discovery.shared.Application;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.model.Job;
import com.netflix.genie.common.model.JobStatus;
import com.netflix.genie.common.model.Job_;
import com.netflix.genie.server.metrics.JobCountManager;
import com.netflix.genie.server.util.NetUtil;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.TypedQuery;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;

import org.apache.commons.configuration.AbstractConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Transactional;

/**
 * Utility class to get number of jobs running on this instance.
 *
 * @author skrishnan
 * @author tgianos
 */
public class JobCountManagerImpl implements JobCountManager {

    private static final Logger LOG = LoggerFactory.getLogger(JobCountManagerImpl.class);

    @PersistenceContext
    private EntityManager em;

    // Config Instance to get all properties
    private final AbstractConfiguration config;

    /**
     * Default Constructor.
     */
    public JobCountManagerImpl() {
        this.config = ConfigurationManager.getConfigInstance();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getNumInstanceJobs() throws GenieException {
        LOG.debug("called");

        return getNumInstanceJobs(null, null, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getNumInstanceJobs(
            final Long minStartTime,
            final Long maxStartTime)
            throws GenieException {
        LOG.debug("called");

        return getNumInstanceJobs(null, minStartTime, maxStartTime);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    //TODO: Move to specification
    public int getNumInstanceJobs(
            final String hostName,
            final Long minStartTime,
            final Long maxStartTime) throws GenieException {
        LOG.debug("called");

        final String finalHostName;
        // initialize host name
        if (StringUtils.isBlank(hostName)) {
            finalHostName = NetUtil.getHostName();
        } else {
            finalHostName = hostName;
        }
        final CriteriaBuilder cb = this.em.getCriteriaBuilder();
        final CriteriaQuery<Long> cq = cb.createQuery(Long.class);
        final Root<Job> j = cq.from(Job.class);
        cq.select(cb.count(j));
        final Predicate runningStatus = cb.equal(j.get(Job_.status), JobStatus.RUNNING);
        final Predicate initStatus = cb.equal(j.get(Job_.status), JobStatus.INIT);
        final List<Predicate> predicates = new ArrayList<>();
        predicates.add(cb.equal(j.get(Job_.hostName), finalHostName));
        predicates.add(cb.or(runningStatus, initStatus));
        if (minStartTime != null) {
            predicates.add(cb.greaterThanOrEqualTo(j.get(Job_.started), new Date(minStartTime)));
        }
        if (maxStartTime != null) {
            predicates.add(cb.lessThan(j.get(Job_.started), new Date(maxStartTime)));
        }
        //documentation says that by default predicate array is conjuncted together
        cq.where(predicates.toArray(new Predicate[predicates.size()]));
        final TypedQuery<Long> query = this.em.createQuery(cq);
        //Downgrading to an int since all the code seems to want ints
        //Don't feel like changing everthing right now can figure out later
        //if need be
        return query.getSingleResult().intValue();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getIdleInstance(
            final long minJobThreshold)
            throws GenieException {
        LOG.debug("called");
        final String localhost = NetUtil.getHostName();

        // Get the App Name from Configuration
        final String appName = this.config.getString("APPNAME", "genie2");
        LOG.info("Using App Name" + appName);

        // naive implementation where we loop through all instances in discovery
        // no need to raise any exceptions here, just return localhost if there
        // is any error
        //TODO: use injection instead of getInstance
        final DiscoveryClient discoveryClient = DiscoveryManager.getInstance()
                .getDiscoveryClient();
        if (discoveryClient == null) {
            LOG.warn("Can't instantiate DiscoveryClient - returning localhost");
            return localhost;
        }
        final Application app = discoveryClient.getApplication(appName);
        if (app == null) {
            LOG.warn("Discovery client can't find genie - returning localhost");
            return localhost;
        }

        for (InstanceInfo instance : app.getInstances()) {
            // only pick instances that are UP
            if (instance.getStatus() == null
                    || instance.getStatus() != InstanceStatus.UP) {
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
        LOG.info("Can't find any host to forward to - returning localhost");
        return localhost;
    }
}
