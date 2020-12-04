/*
 *
 *  Copyright 2020 Netflix, Inc.
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
package com.netflix.genie.common.internal.util;

import com.amazonaws.util.EC2MetadataUtils;
import io.awspring.cloud.context.support.env.AwsCloudEnvironmentCheckUtils;
import org.apache.commons.lang3.StringUtils;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Static utility class to determine the local hostname.
 *
 * @author mprimi
 * @since 4.0.0
 */
public final class HostnameUtil {

    private HostnameUtil() {
    }

    /**
     * Get the local hostname string.
     * This implementation actually return an IP address string.
     *
     * @return a hostname string
     * @throws UnknownHostException if hostname resolution fails
     */
    public static String getHostname() throws UnknownHostException {
        final String hostname;
        if (AwsCloudEnvironmentCheckUtils.isRunningOnCloudEnvironment()) {
            hostname = EC2MetadataUtils.getPrivateIpAddress();
        } else {
            // Fallback if not on AWS
            hostname = InetAddress.getLocalHost().getCanonicalHostName();
        }

        if (StringUtils.isBlank(hostname)) {
            throw new IllegalStateException("Unable to create a Genie Host Info instance as hostname is blank");
        }

        return hostname;
    }
}
