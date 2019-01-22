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
import com.netflix.genie.common.internal.services.impl.FileSystemJobArchiverImpl;
import com.netflix.genie.common.internal.services.impl.JobArchiveServiceImpl;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
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
     * Constant allowing developers to reference the precedence in their own configuration files.
     *
     * @see Ordered
     */
    public static final int FILE_SYSTEM_JOB_ARCHIVER_PRECEDENCE = Ordered.LOWEST_PRECEDENCE - 20;

    /**
     * Provide a {@link JobArchiver} implementation that will copy from one place on the filesystem to another.
     *
     * @return A {@link FileSystemJobArchiverImpl} instance
     */
    @Bean
    @Order(FILE_SYSTEM_JOB_ARCHIVER_PRECEDENCE)
    public FileSystemJobArchiverImpl fileSystemJobArchiver() {
        return new FileSystemJobArchiverImpl();
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
