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

import com.netflix.config.ConfigurationManager;
import com.netflix.genie.common.exceptions.CloudServiceException;
import java.net.HttpURLConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BaseServiceFactory class that others will inherit from.
 *
 * @author skrishnan
 */
public abstract class BaseServiceFactory {

    private static final Logger LOG = LoggerFactory
            .getLogger(BaseServiceFactory.class);

    /**
     * Utility function to instantiate service implementation from a property.
     *
     * @param propName property name whose value contains FQCN of class to
     * instantiate
     * @return object instantiated from propName
     * @throws CloudServiceException if there is any error
     */
    public static Object instantiateFromProperty(final String propName)
            throws CloudServiceException {
        String implFQCN = ConfigurationManager.getConfigInstance().getString(propName);
        if (implFQCN == null) {
            String msg = "Configuration error - " + propName + " is not found";
            LOG.error(msg);
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        } else {
            try {
                LOG.info("Instantiating : " + implFQCN);
                return Class.forName(implFQCN).newInstance();
            } catch (Exception e) {
                String msg = "Can't instantiate " + implFQCN
                        + " for property: " + propName;
                LOG.error(msg, e);
                throw new CloudServiceException(
                        HttpURLConnection.HTTP_INTERNAL_ERROR, msg, e);
            }
        }
    }
}
