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
package com.netflix.genie.server.services;

import com.netflix.genie.common.exceptions.CloudServiceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory class to instantiate implementation of the various services.
 *
 * @author skrishnan
 * @author amsharma
 */
//TODO: Get rid of this for some DI framework
public final class ConfigServiceFactory extends BaseServiceFactory {

    private static final  Logger LOG = LoggerFactory
            .getLogger(ConfigServiceFactory.class);

    // handle to the CommandConfigService
    private static volatile ClusterConfigService clusterConfigService;

    // handle to the ClusterLoadBalancer
    private static volatile ClusterLoadBalancer clusterLoadBalancer;

    // handle to the ApplicationConfigService
    private static volatile ApplicationConfigService applicationConfigService;

    // handle to the CommandConfigService
    private static volatile CommandConfigService commandConfigService;

    // never called
    private ConfigServiceFactory() {
    }

    /**
     * Get the singleton cluster config service impl.
     *
     * @return singleton cluster config service impl
     * @throws CloudServiceException
     */
    public static ClusterConfigService getClusterConfigImpl()
            throws CloudServiceException {
        LOG.info("called");

        // instantiate the impl if it hasn't been already
        if (clusterConfigService == null) {
            synchronized (ConfigServiceFactory.class) {
                // double-checked locking
                if (clusterConfigService == null) {
                    clusterConfigService = (ClusterConfigService) instantiateFromProperty("netflix.genie.server.clusterConfigImpl");
                }
            }
        }

        // return generated or cached impl
        return clusterConfigService;
    }

    /**
     * Get instance of the configured cluster load balancer.
     *
     * @return singleton cluster load balancer impl
     * @throws CloudServiceException
     */
    public static ClusterLoadBalancer getClusterLoadBalancer()
            throws CloudServiceException {
        LOG.info("called");

        // instantiate the impl if it hasn't been already
        if (clusterLoadBalancer == null) {
            synchronized (ConfigServiceFactory.class) {
                // double-checked locking
                if (clusterLoadBalancer == null) {
                    clusterLoadBalancer = (ClusterLoadBalancer) instantiateFromProperty("netflix.genie.server.clusterLoadBalancerImpl");
                }
            }
        }

        // return generated or cached impl
        return clusterLoadBalancer;
    }

    /**
     * Get the singleton application config service impl.
     *
     * @return singleton application config service impl
     * @throws CloudServiceException
     */
    public static ApplicationConfigService getApplicationConfigImpl()
            throws CloudServiceException {
        LOG.info("called");

        // instantiate the impl if it hasn't been already
        if (applicationConfigService == null) {
            synchronized (ConfigServiceFactory.class) {
                // double-checked locking
                if (applicationConfigService == null) {
                    applicationConfigService = (ApplicationConfigService) instantiateFromProperty("netflix.genie.server.applicationConfigImpl");
                }
            }
        }

        // return generated or cached impl
        return applicationConfigService;
    }

    /**
     * Get the singleton command config service impl.
     *
     * @return singleton command config service impl
     * @throws CloudServiceException
     */
    public static CommandConfigService getCommandConfigImpl()
            throws CloudServiceException {
        LOG.info("called");

        // instantiate the impl if it hasn't been already
        if (commandConfigService == null) {
            synchronized (ConfigServiceFactory.class) {
                // double-checked locking
                if (commandConfigService == null) {
                    commandConfigService = (CommandConfigService) instantiateFromProperty("netflix.genie.server.commandConfigImpl");
                }
            }
        }

        // return generated or cached impl
        return commandConfigService;
    }
}
