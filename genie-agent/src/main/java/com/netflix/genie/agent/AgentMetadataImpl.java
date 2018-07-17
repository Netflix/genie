/*
 *
 *  Copyright 2018 Netflix, Inc.
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

package com.netflix.genie.agent;

import com.netflix.genie.GenieAgentApplication;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Implementation of AgentMetadata.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
@Component
@Getter
@Lazy
class AgentMetadataImpl implements AgentMetadata {

    private static final String FALLBACK_STRING = "unknown";
    private final String agentVersion;
    private final String agentHostName;
    private final String agentPid;

    AgentMetadataImpl() {
        this.agentVersion = getAgentVersionOrFallback();
        this.agentHostName = getAgentHostNameOrFallback();
        this.agentPid = getAgentPidOrFallback();
    }

    private static String getAgentVersionOrFallback() {
        final String agentVersion = GenieAgentApplication.class.getPackage().getImplementationVersion();
        if (!StringUtils.isBlank(agentVersion)) {
            return agentVersion;
        }
        log.warn("Failed to retrieve agent version");
        return FALLBACK_STRING;
    }

    private static String getAgentHostNameOrFallback() {
        try {
            final String hostName = InetAddress.getLocalHost().getHostName();
            if (!StringUtils.isBlank(hostName)) {
                return hostName;
            }
        } catch (final UnknownHostException e) {
            log.warn("Failed to retrieve local host name", e);
        }
        return FALLBACK_STRING;
    }

    private static String getAgentPidOrFallback() {
        final String jvmId = ManagementFactory.getRuntimeMXBean().getName();
        final Matcher pidPatternMatcher = Pattern.compile("(\\d+)@.*").matcher(jvmId);
        if (pidPatternMatcher.matches() && !StringUtils.isBlank(pidPatternMatcher.group(1))) {
            return pidPatternMatcher.group(1);
        }
        log.warn("Failed to retrieve agent PID (JVM id: {})", jvmId);
        return FALLBACK_STRING;
    }
}
