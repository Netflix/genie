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
package com.netflix.genie.server.util;

import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieServerException;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import javax.inject.Named;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Utility class to return appropriate host names and S3 locations.
 *
 * @author skrishnan
 */
@Named
public final class NetUtil {

    private static final Logger LOG = LoggerFactory.getLogger(NetUtil.class);

    // The instance meta-data uri's for public and private host/ip's
    // More info about EC2's instance metadata API is here:
    // http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/AESDG-chapter-instancedata.html
    private static final String PUBLIC_HOSTNAME_URI
            = "http://169.254.169.254/latest/meta-data/public-hostname";
    private static final String LOCAL_IPV4_URI
            = "http://169.254.169.254/latest/meta-data/local-ipv4";

    @Value("${com.netflix.genie.server.s3.archive.location:null}")
    private String s3ArchiveLocation;
    @Value("${com.netflix.genie.server.host:null}")
    private String hostNameProperty;
    @Value("${netflix.datacenter:null}")
    private String dataCenter;

    private  String cloudHostName;
    private String dcHostName;

    /**
     * Returns the s3 location where job logs will be archived.
     *
     * @param jobID to build archive location for
     * @return s3 location
     */
    public String getArchiveURI(final String jobID) {
        LOG.debug("called for jobID: " + jobID);
        if (StringUtils.isNotBlank(this.s3ArchiveLocation)) {
            return this.s3ArchiveLocation + "/" + jobID;
        } else {
            return null;
        }
    }

    /**
     * Return either the cloud or dc host name, depending on the datacenter. If
     * the property com.netflix.genie.server.host is set, that value will always be
     * returned. If the property is not set, then the instance metadata will be
     * used in the cloud, or InetAddress.getLocalHost() will be used in the DC.
     *
     * @return host name
     * @throws GenieException For any error.
     */
    public String getHostName() throws GenieException {
        LOG.debug("called");

        // check the fast property first
        if (StringUtils.isNotBlank(this.hostNameProperty)) {
            return hostNameProperty;
        }

        // if hostName is not set by property, figure it out based on the datacenter
        String hostName;
        if (this.dataCenter != null && this.dataCenter.equals("cloud")) {
            hostName = getCloudHostName();
        } else {
            hostName = getDCHostName();
        }

        if (hostName == null || hostName.isEmpty()) {
            final String msg = "Can't figure out host name for instance";
            LOG.error(msg);
            throw new GenieServerException(msg);
        }

        return hostName;
    }

    private String getCloudHostName() throws GenieException {
        LOG.debug("called");

        if (StringUtils.isNotBlank(this.cloudHostName)) {
            return cloudHostName;
        }

        // gets the ec2 public hostname, if available
        try {
            cloudHostName = httpGet(PUBLIC_HOSTNAME_URI);
        } catch (final IOException ioe) {
            final String msg = "Unable to get public hostname from instance metadata";
            LOG.error(msg, ioe);
            throw new GenieServerException(msg, ioe);
        }
        if (StringUtils.isBlank(this.cloudHostName)) {
            try {
                this.cloudHostName = httpGet(LOCAL_IPV4_URI);
            } catch (final IOException ioe) {
                final String msg = "Unable to get local IP from instance metadata";
                LOG.error(msg, ioe);
                throw new GenieServerException(msg, ioe);
            }
        }
        LOG.info("cloudHostName=" + this.cloudHostName);

        return this.cloudHostName;
    }

    /**
     * Returns the response from an HTTP GET call if it succeeds, null
     * otherwise.
     *
     * @param uri The URI to execute the HTTP GET on
     * @return response from an HTTP GET call if it succeeds, null otherwise
     * @throws IOException if there was an error with the HTTP request
     */
    private String httpGet(final String uri) throws IOException {
        String response = null;
        //TODO: Use one of other http clients to remove dependency
        final HttpClient client = new HttpClient();
        final HttpMethod method = new GetMethod(uri);
        client.executeMethod(method);
        final int status = method.getStatusCode();
        if (status == HttpURLConnection.HTTP_OK) {
            response = method.getResponseBodyAsString();
        }
        return response;
    }

    private String getDCHostName() throws GenieException {
        LOG.debug("called");

        if (StringUtils.isNotBlank(this.dcHostName)) {
            return this.dcHostName;
        }

        try {
            // gets the local instance hostname
            final InetAddress addr = InetAddress.getLocalHost();
            this.dcHostName = addr.getCanonicalHostName();
            return this.dcHostName;
        } catch (final UnknownHostException e) {
            final String msg = "Unable to get the hostname";
            LOG.error(msg, e);
            throw new GenieServerException(msg, e);
        }
    }
}
