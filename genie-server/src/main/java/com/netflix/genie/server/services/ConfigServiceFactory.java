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

package com.netflix.genie.server.services;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.genie.common.exceptions.CloudServiceException;

/**
 * Factory class to instantiate implementation of the various services.
 *
 * @author skrishnan
 * @author amsharma
 */
public final class ConfigServiceFactory extends BaseServiceFactory {

    private static Logger logger = LoggerFactory
            .getLogger(ConfigServiceFactory.class);

    // handle to the HiveConfigService
    private static volatile HiveConfigService hiveConfigService;

    // handle to the PigConfigService
    private static volatile PigConfigService pigConfigService;

    // handle to the ClusterConfigServiceOld
    private static volatile ClusterConfigServiceOld clusterConfigServiceOld;
    
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
     * Get the singleton hive config service impl.
     *
     * @return singleton hive config service impl
     * @throws CloudServiceException
     */
    public static HiveConfigService getHiveConfigImpl()
            throws CloudServiceException {
        logger.info("called");

        // instantiate the impl if it hasn't been already
        if (hiveConfigService == null) {
            synchronized (ConfigServiceFactory.class) {
                // double-checked locking
                if (hiveConfigService == null) {
                    hiveConfigService = (HiveConfigService)
                            instantiateFromProperty("netflix.genie.server.hiveConfigImpl");
                }
            }
        }

        // return generated or cached impl
        return hiveConfigService;
    }

    /**
     * Get the singleton pig config service impl.
     *
     * @return singleton pig config service impl
     * @throws CloudServiceException
     */
    public static PigConfigService getPigConfigImpl()
            throws CloudServiceException {
        logger.info("called");

        // instantiate the impl if it hasn't been already
        if (pigConfigService == null) {
            synchronized (ConfigServiceFactory.class) {
                // double-checked locking
                if (pigConfigService == null) {
                    pigConfigService = (PigConfigService)
                            instantiateFromProperty("netflix.genie.server.pigConfigImpl");
                }
            }
        }

        // return generated or cached impl
        return pigConfigService;
    }

    /**
     * Get the singleton cluster config service impl.
     *
     * @return singleton cluster config service impl
     * @throws CloudServiceException
     */
    public static ClusterConfigServiceOld getClusterConfigImplOld()
            throws CloudServiceException {
        logger.info("called");

        // instantiate the impl if it hasn't been already
        if (clusterConfigServiceOld == null) {
            synchronized (ConfigServiceFactory.class) {
                // double-checked locking
                if (clusterConfigServiceOld == null) {
                    clusterConfigServiceOld = (ClusterConfigServiceOld)
                            instantiateFromProperty("netflix.genie.server.clusterConfigImplOld");
                }
            }
        }

        // return generated or cached impl
        return clusterConfigServiceOld;
    }
    
    /**
     * Get the singleton cluster config service impl.
     *
     * @return singleton cluster config service impl
     * @throws CloudServiceException
     */
    public static ClusterConfigService getClusterConfigImpl()
            throws CloudServiceException {
        logger.info("called");

        // instantiate the impl if it hasn't been already
        if (clusterConfigService == null) {
            synchronized (ConfigServiceFactory.class) {
                // double-checked locking
                if (clusterConfigService == null) {
                    clusterConfigService = (ClusterConfigService)
                            instantiateFromProperty("netflix.genie.server.clusterConfigImpl");
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
        logger.info("called");

        // instantiate the impl if it hasn't been already
        if (clusterLoadBalancer == null) {
            synchronized (ConfigServiceFactory.class) {
                // double-checked locking
                if (clusterLoadBalancer == null) {
                    clusterLoadBalancer = (ClusterLoadBalancer)
                            instantiateFromProperty("netflix.genie.server.clusterLoadBalancerImpl");
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
        logger.info("called");

        // instantiate the impl if it hasn't been already
        if (applicationConfigService == null) {
            synchronized (ConfigServiceFactory.class) {
                // double-checked locking
                if (applicationConfigService == null) {
                    applicationConfigService = (ApplicationConfigService)
                            instantiateFromProperty("netflix.genie.server.applicationConfigImpl");
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
        logger.info("called");

        // instantiate the impl if it hasn't been already
        if (commandConfigService == null) {
            synchronized (ConfigServiceFactory.class) {
                // double-checked locking
                if (commandConfigService == null) {
                    commandConfigService = (CommandConfigService)
                            instantiateFromProperty("netflix.genie.server.commandConfigImpl");
                }
            }
        }

        // return generated or cached impl
        return commandConfigService;
    }
}
