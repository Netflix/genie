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

package com.netflix.genie.server.util;

import java.net.HttpURLConnection;
import java.net.InetAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.config.ConfigurationManager;
import com.netflix.genie.common.exceptions.CloudServiceException;
import com.netflix.genie.server.services.ExecutionServiceFactory;

/**
 * Utility class to return appropriate hostnames and S3 locations.
 *
 * @author skrishnan
 */
public final class NetUtil {

    private static String publicHostName;
    private static String localHostName;

    private static Logger logger = LoggerFactory.getLogger(NetUtil.class);

    private NetUtil() {
        // never called
    }

    /**
     * Returns the s3 location where job logs will be archived.
     *
     * @param jobID
     *            to build archive location for
     * @return s3 location
     * @throws CloudServiceException
     */
    public static String getArchiveURI(String jobID) {
        logger.debug("called for jobID: " + jobID);
        String s3ArchiveLocation = ExecutionServiceFactory.getJobEnv().get(
                "S3_ARCHIVE_LOCATION");
        if (s3ArchiveLocation != null) {
            return s3ArchiveLocation + "/" + jobID;
        } else {
            return null;
        }
    }

    /**
     * Return either the public or local dns name, depending on the datacenter.
     * If the property netflix.genie.server.host is set, that value will always be returned.
     * If the property is not set, then the environment variable EC2_PUBLIC_HOSTNAME will
     * be used in the cloud, or InetAddress.getLocalHost() will be used in the DC.
     *
     * @return host name
     */
    public static String getHostName() throws CloudServiceException {
        logger.debug("called");

        // check the fast property first
        String hostName = ConfigurationManager.getConfigInstance().getString(
                "netflix.genie.server.host");
        if ((hostName != null) && (!hostName.isEmpty())) {
            return hostName;
        }

        // if hostName is not set by property, figure it out based on the datacenter
        String dc = ConfigurationManager.getConfigInstance().getString(
                "netflix.datacenter");
        if ((dc != null) && dc.equals("cloud")) {
            hostName = getPublicHostName();
        } else {
            hostName = getLocalHostName();
        }

        if ((hostName == null) || (hostName.isEmpty())) {
            String msg = "Can't figure out host name for instance";
            logger.error(msg);
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        }

        return hostName;
    }

    private static String getPublicHostName() throws CloudServiceException {
        logger.debug("called");

        if ((publicHostName != null) && (!publicHostName.isEmpty())) {
            return publicHostName;
        }

        // gets the ec2 public hostname
        publicHostName = System.getenv("EC2_PUBLIC_HOSTNAME");
        logger.debug("publicHostName=" + publicHostName);

        return publicHostName;
    }

    private static String getLocalHostName() throws CloudServiceException {
        logger.debug("called");

        if ((localHostName != null) && (!localHostName.isEmpty())) {
            return localHostName;
        }

        try {
            // gets the local instance hostname
            InetAddress addr = InetAddress.getLocalHost();
            localHostName = addr.getCanonicalHostName();
        } catch (Exception e) {
            String msg = "Unable to get the hostname";
            logger.error(msg, e);
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, msg, e);
        }

        return localHostName;
    }
}
