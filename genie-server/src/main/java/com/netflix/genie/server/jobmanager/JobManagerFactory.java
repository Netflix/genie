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
package com.netflix.genie.server.jobmanager;

import com.netflix.genie.common.exceptions.CloudServiceException;
import com.netflix.genie.common.messages.ClusterConfigResponse;
import com.netflix.genie.common.model.Cluster;
import com.netflix.genie.common.model.Job;
import com.netflix.genie.server.services.ClusterConfigService;
import com.netflix.genie.server.services.ClusterLoadBalancer;
import com.netflix.genie.server.services.ConfigServiceFactory;
import java.net.HttpURLConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory class to instantiate individual job managers.
 *
 * @author skrishnan
 * @author tgianos
 */
public final class JobManagerFactory {

    private static final Logger LOG = LoggerFactory
            .getLogger(JobManagerFactory.class);

    /**
     * The service to discover clusters.
     */
    //TODO: this should be managed via DI framework of some type
    private final ClusterConfigService ccs;

    /**
     * Reference to the cluster load balancer implementation.
     */
    private final ClusterLoadBalancer clb;

    /**
     * Default constructor.
     *
     * @throws CloudServiceException
     */
    public JobManagerFactory() throws CloudServiceException {
        this.ccs = ConfigServiceFactory.getClusterConfigImpl();
        this.clb = ConfigServiceFactory.getClusterLoadBalancer();
    }

    /**
     * Returns the right job manager for the job type.
     *
     * @param job The job this manager will be managing
     * @return instance of the appropriate job manager
     * @throws CloudServiceException
     */
    public JobManager getJobManager(final Job job) throws CloudServiceException {
        LOG.info("called");

        final Cluster cluster = getCluster(job);
        final String className = cluster.getJobManager();

        try {
            final Class jobManagerClass = Class.forName(className);
            final Object instance = jobManagerClass.getConstructor(Cluster.class).newInstance(cluster);
            if (instance instanceof JobManager) {
                return (JobManager) instance;
            } else {
                final String msg = className + " is not of type JobManager. Unable to continue.";
                LOG.error(msg);
                throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST, msg);
            }
        } catch (final Exception e) {
            final String msg = "Unable to create job manager for class name " + className;
            LOG.error(msg, e);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }
    }

    /**
     * Figure out an appropriate cluster to run this job<br>
     * Cluster selection is done based on tags, command and application.
     *
     * @param job job info for this job
     * @return cluster element to use for running this job
     * @throws CloudServiceException if there is any error finding a cluster for
     * this job
     */
    private Cluster getCluster(final Job job) throws CloudServiceException {
        LOG.info("called");

        if (job == null) {
            final String msg = "No job entered. Unable to continue";
            LOG.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }

        final ClusterConfigResponse ccr = ccs.getClusterConfig(
                job.getApplicationId(),
                job.getApplicationName(),
                job.getCommandId(),
                job.getCommandName(),
                job.getClusterCriteriaList());

        // return selected instance
        return clb.selectCluster(ccr.getClusterConfigs());
    }
}
