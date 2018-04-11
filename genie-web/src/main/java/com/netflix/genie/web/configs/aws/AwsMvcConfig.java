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

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cloud.aws.context.annotation.ConditionalOnAwsCloudEnvironment;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;

/**
 * Beans and configuration specifically for MVC on AWS.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Configuration
@ConditionalOnAwsCloudEnvironment
@Slf4j
public class AwsMvcConfig {

    // See: http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/AESDG-chapter-instancedata.html
    static final String PUBLIC_HOSTNAME_GET = "http://169.254.169.254/latest/meta-data/public-hostname";
    static final String LOCAL_IPV4_HOSTNAME_GET = "http://169.254.169.254/latest/meta-data/local-ipv4";

    /**
     * Get the host name for this application by calling the AWS metadata endpoints. Overrides default implementation
     * which defaults to using InetAddress class. Only active when profile enabled.
     *
     * @param restTemplate The rest template to use to call the Amazon endpoints
     * @return The hostname
     */
    @Bean
    public String hostName(@Qualifier("genieRestTemplate") final RestTemplate restTemplate) {
        String result;
        try {
            result = restTemplate.getForObject(PUBLIC_HOSTNAME_GET, String.class);
            log.debug("AWS Public Hostname: {}", result);
        } catch (Exception e) {
            result = restTemplate.getForObject(LOCAL_IPV4_HOSTNAME_GET, String.class);
            log.debug("AWS IPV4 Hostname: {}", result);
        }
        return result;
    }
}
