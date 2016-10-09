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
package com.netflix.genie.web.configs;

import com.netflix.genie.core.properties.JobsProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration for creating beans for Genie Properties.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Configuration
public class PropertiesConfig {

    /**
     * All the properties related to configuring and running jobs.
     *
     * @return The jobs properties structure
     */
    @Bean
    @ConfigurationProperties("genie.jobs")
    public JobsProperties jobsProperties() {
        return new JobsProperties();
    }
}
