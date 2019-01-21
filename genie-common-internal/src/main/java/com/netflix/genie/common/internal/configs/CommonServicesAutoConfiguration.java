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

import com.netflix.genie.common.internal.services.JobArchiveService;
import com.netflix.genie.common.internal.services.JobArchiver;
import com.netflix.genie.common.internal.services.impl.JobArchiveServiceImpl;
import com.netflix.genie.common.internal.services.impl.NoOpJobArchiverImpl;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;

import java.util.List;

/**
 * Auto configuration of any services that are common to both the agent and the server.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Configuration
public class CommonServicesAutoConfiguration {

    /**
     * Provide a dummy no-op job archiver for now.
     *
     * @return A {@link NoOpJobArchiverImpl} instance
     */
    @Bean
    @Order // defaults to lowest precedence
    public NoOpJobArchiverImpl noOpJobArchiver() {
        return new NoOpJobArchiverImpl();
    }

    /**
     * Provide a default {@link JobArchiveService} if no override is defined.
     *
     * @param jobArchivers The ordered available {@link JobArchiver} implementations in the system
     * @return A {@link JobArchiveServiceImpl} instance
     */
    @Bean
    @ConditionalOnMissingBean(JobArchiveService.class)
    public JobArchiveService jobArchiveService(final List<JobArchiver> jobArchivers) {
        return new JobArchiveServiceImpl(jobArchivers);
    }
}
