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
package com.netflix.genie.server.util;

import com.netflix.config.ConfigurationManager;
import com.netflix.genie.common.exceptions.GenieException;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to return appropriate host names and S3 locations.
 *
 * @author skrishnan
 */
public final class NetUtil {

    private static String cloudHostName;
    private static String dcHostName;

    private static final Logger LOG = LoggerFactory.getLogger(NetUtil.class);

    // The instance meta-data uri's for public and private host/ip's
    // More info about EC2's instance metadata API is here:
    // http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/AESDG-chapter-instancedata.html
    private static final String PUBLIC_HOSTNAME_URI
            = "http://169.254.169.254/latest/meta-data/public-hostname";
    private static final String LOCAL_IPV4_URI
            = "http://169.254.169.254/latest/meta-data/local-ipv4";

    private NetUtil() {
        // never called
    }

    /**
     * Returns the s3 location where job logs will be archived.
     *
     * @param jobID to build archive location for
     * @return s3 location
     */
    public static String getArchiveURI(final String jobID) {
        LOG.debug("called for jobID: " + jobID);
        String s3ArchiveLocation = ConfigurationManager.getConfigInstance()
                .getString("netflix.genie.server.s3.archive.location");
        if ((s3ArchiveLocation != null) && (!s3ArchiveLocation.isEmpty())) {
            return s3ArchiveLocation + "/" + jobID;
        } else {
            return null;
        }
    }

    /**
     * Return either the cloud or dc host name, depending on the datacenter. If
     * the property netflix.genie.server.host is set, that value will always be
     * returned. If the property is not set, then the instance metadata will be
     * used in the cloud, or InetAddress.getLocalHost() will be used in the DC.
     *
     * @return host name
     * @throws com.netflix.genie.common.exceptions.GenieException
     */
    public static String getHostName() throws GenieException {
        LOG.debug("called");

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
            hostName = getCloudHostName();
        } else {
            hostName = getDCHostName();
        }

        if ((hostName == null) || (hostName.isEmpty())) {
            String msg = "Can't figure out host name for instance";
            LOG.error(msg);
            throw new GenieException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        }

        return hostName;
    }

    private static String getCloudHostName() throws GenieException {
        LOG.debug("called");

        if ((cloudHostName != null) && (!cloudHostName.isEmpty())) {
            return cloudHostName;
        }

        // gets the ec2 public hostname, if available
        try {
            cloudHostName = httpGet(PUBLIC_HOSTNAME_URI);
        } catch (IOException ioe) {
            String msg = "Unable to get public hostname from instance metadata";
            LOG.error(msg, ioe);
            throw new GenieException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, msg, ioe);
        }
        if ((cloudHostName == null) || (cloudHostName.isEmpty())) {
            try {
                cloudHostName = httpGet(LOCAL_IPV4_URI);
            } catch (IOException ioe) {
                String msg = "Unable to get local IP from instance metadata";
                LOG.error(msg, ioe);
                throw new GenieException(
                        HttpURLConnection.HTTP_INTERNAL_ERROR, msg, ioe);
            }
        }
        LOG.info("cloudHostName=" + cloudHostName);

        return cloudHostName;
    }

    /**
     * Returns the response from an HTTP GET call if it succeeds, null
     * otherwise.
     *
     * @param uri The URI to execute the HTTP GET on
     * @return response from an HTTP GET call if it succeeds, null otherwise
     * @throws IOException if there was an error with the HTTP request
     */
    private static String httpGet(String uri) throws IOException {
        String response = null;
        //TODO: Use one of other http clients to remove dependency
        HttpClient client = new HttpClient();
        HttpMethod method = new GetMethod(uri);
        client.executeMethod(method);
        int status = method.getStatusCode();
        if (status == HttpURLConnection.HTTP_OK) {
            response = method.getResponseBodyAsString();
        }
        return response;
    }

    private static String getDCHostName() throws GenieException {
        LOG.debug("called");

        if ((dcHostName != null) && (!dcHostName.isEmpty())) {
            return dcHostName;
        }

        try {
            // gets the local instance hostname
            InetAddress addr = InetAddress.getLocalHost();
            dcHostName = addr.getCanonicalHostName();
        } catch (final UnknownHostException e) {
            String msg = "Unable to get the hostname";
            LOG.error(msg, e);
            throw new GenieException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, msg, e);
        }

        return dcHostName;
    }
}
