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

import io.micrometer.common.util.StringUtils;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Scanner;

public final class HostnameUtil {

    private static final String EC2_METADATA_URL = "http://169.254.169.254/latest/meta-data/local-ipv4";

    private HostnameUtil() {
    }

    public static String getHostname() throws UnknownHostException {
        final String hostname;
        if (isRunningOnAws()) {
            hostname = getEc2PrivateIpAddress();
        } else {
            hostname = InetAddress.getLocalHost().getCanonicalHostName();
        }

        if (StringUtils.isBlank(hostname)) {
            throw new IllegalStateException("Unable to create a Genie Host Info instance as hostname is blank");
        }

        return hostname;
    }

    private static boolean isRunningOnAws() {
        try {
            HttpURLConnection connection = (HttpURLConnection) new URL(EC2_METADATA_URL).openConnection();
            connection.setRequestMethod("GET");
            connection.setConnectTimeout(1000);
            connection.setReadTimeout(1000);
            return connection.getResponseCode() == 200;
        } catch (IOException e) {
            return false;
        }
    }

    private static String getEc2PrivateIpAddress() {
        try {
            HttpURLConnection connection = (HttpURLConnection) new URL(EC2_METADATA_URL).openConnection();
            connection.setRequestMethod("GET");
            connection.setConnectTimeout(1000);
            connection.setReadTimeout(1000);
            if (connection.getResponseCode() == 200) {
                try (Scanner scanner = new Scanner(connection.getInputStream())) {
                    return scanner.useDelimiter("\\A").next();
                }
            }
        } catch (IOException e) {
            // Handle exception
        }
        return null;
    }
}
