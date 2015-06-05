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
package com.netflix.genie.server.jobmanager;

import com.netflix.config.ConfigurationManager;
import com.netflix.genie.common.exceptions.GenieBadRequestException;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.model.Cluster;
import com.netflix.genie.common.model.Job;
import com.netflix.genie.server.services.ClusterConfigService;
import com.netflix.genie.server.services.ClusterLoadBalancer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

/**
 * Factory class to instantiate individual job managers.
 *
 * @author skrishnan
 * @author tgianos
 * @author amsharma
 */
public class JobManagerFactory implements ApplicationContextAware {

    private static final Logger LOG = LoggerFactory.getLogger(JobManagerFactory.class);
    private final ClusterConfigService ccs;
    private final ClusterLoadBalancer clb;
    private ApplicationContext context;

    /**
     * Default constructor.
     *
     * @param ccs The cluster config service to use
     * @param clb The clb to use
     */
    public JobManagerFactory(
            final ClusterConfigService ccs,
            final ClusterLoadBalancer clb) {
        this.ccs = ccs;
        this.clb = clb;
    }

    /**
     * Returns the right job manager for the job type.
     *
     * @param job The job this manager will be managing
     * @return instance of the appropriate job manager
     * @throws GenieException On error
     */
    public JobManager getJobManager(final Job job) throws GenieException {
        LOG.info("called");

        if (job == null) {
            final String msg = "No job entered. Unable to continue";
            LOG.error(msg);
            throw new GeniePreconditionException(msg);
        }

        // Figure out a cluster to run this job. Cluster selection is done based on
        // ClusterCriteria tags and Command tags specified in the job.
        final Cluster cluster = this.clb.selectCluster(this.ccs.chooseClusterForJob(job.getId()));
        final String className = ConfigurationManager.getConfigInstance()
                .getString("com.netflix.genie.server.job.manager." + cluster.getClusterType() + ".impl");

        try {
            final Class jobManagerClass = Class.forName(className);
            final Object instance = this.context.getBean(jobManagerClass);
            if (instance instanceof JobManager) {
                final JobManager jobManager = (JobManager) instance;
                jobManager.init(job, cluster);
                return jobManager;
            } else {
                final String msg = className + " is not of type JobManager. Unable to continue.";
                LOG.error(msg);
                throw new GeniePreconditionException(msg);
            }
        } catch (final ClassNotFoundException | BeansException e) {
            final String msg = "Unable to create job manager for class name " + className;
            LOG.error(msg, e);
            throw new GenieBadRequestException(msg);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setApplicationContext(final ApplicationContext appContext) throws BeansException {
        this.context = appContext;
    }
}
