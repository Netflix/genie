/*
 *
 *  Copyright 2019 Netflix, Inc.
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
package com.netflix.genie.common.internal.configs;

import com.netflix.genie.common.internal.dtos.v4.converters.JobDirectoryManifestProtoConverter;
import com.netflix.genie.common.internal.dtos.v4.converters.JobServiceProtoConverter;
import com.netflix.genie.common.util.GenieObjectMapper;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Auto configuration of components common to both the agent and the server.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Configuration
public class ProtoConvertersAutoConfiguration {

    /**
     * Provide a bean for proto conversion of job service objects if one isn't already defined.
     *
     * @return A {@link JobServiceProtoConverter} instance.
     */
    @Bean
    @ConditionalOnMissingBean(JobServiceProtoConverter.class)
    public JobServiceProtoConverter jobServiceProtoConverter() {
        return new JobServiceProtoConverter();
    }

    /**
     * Provide a bean for proto conversion of agent job directory manifest objects if one isn't already defined.
     *
     * @return A {@link JobDirectoryManifestProtoConverter} instance.
     */
    @Bean
    @ConditionalOnMissingBean(JobDirectoryManifestProtoConverter.class)
    public JobDirectoryManifestProtoConverter jobDirectoryManifestProtoConverter() {
        return new JobDirectoryManifestProtoConverter(GenieObjectMapper.getMapper());
    }
}
