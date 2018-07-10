/*
 *
 *  Copyright 2016 Netflix, Inc.
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
package com.netflix.genie.web.configs.aws;

import com.amazonaws.util.EC2MetadataUtils;
import com.netflix.genie.common.internal.util.GenieHostInfo;
import com.netflix.genie.web.configs.GenieApiAutoConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.cloud.aws.autoconfigure.context.ContextCredentialsAutoConfiguration;
import org.springframework.cloud.aws.context.annotation.ConditionalOnAwsCloudEnvironment;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Beans and configuration specifically for MVC on AWS.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Configuration
@AutoConfigureAfter(ContextCredentialsAutoConfiguration.class)
@AutoConfigureBefore(GenieApiAutoConfiguration.class)
@ConditionalOnAwsCloudEnvironment
@Slf4j
public class GenieAwsApiAutoConfiguration {

    /**
     * Create an instance of {@link GenieHostInfo} using the EC2 metadata service as we're deployed in an AWS cloud
     * environment.
     *
     * @return The {@link GenieHostInfo} instance
     * @throws UnknownHostException If all EC2 host instance calculation AND local resolution can't determine a hostname
     * @throws IllegalStateException If an instance can't be created
     */
    @Bean
    @ConditionalOnMissingBean(GenieHostInfo.class)
    public GenieHostInfo genieHostInfo() throws UnknownHostException {
        final String ec2LocalHostName = EC2MetadataUtils.getLocalHostName();
        if (StringUtils.isNotBlank(ec2LocalHostName)) {
            return new GenieHostInfo(ec2LocalHostName);
        }

        final String ec2Ipv4Address = EC2MetadataUtils.getPrivateIpAddress();
        if (StringUtils.isNotBlank(ec2Ipv4Address)) {
            return new GenieHostInfo(ec2Ipv4Address);
        }

        final String localHostname = InetAddress.getLocalHost().getCanonicalHostName();
        if (StringUtils.isNotBlank(localHostname)) {
            return new GenieHostInfo(localHostname);
        }

        throw new IllegalStateException("Unable to resolve Genie host info");
    }
}
