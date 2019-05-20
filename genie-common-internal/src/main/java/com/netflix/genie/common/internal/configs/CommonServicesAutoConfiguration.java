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

import com.netflix.genie.common.internal.dto.DirectoryManifest;
import com.netflix.genie.common.internal.jobs.JobConstants;
import com.netflix.genie.common.internal.services.JobArchiveService;
import com.netflix.genie.common.internal.services.JobArchiver;
import com.netflix.genie.common.internal.services.JobDirectoryManifestService;
import com.netflix.genie.common.internal.services.impl.FileSystemJobArchiverImpl;
import com.netflix.genie.common.internal.services.impl.JobArchiveServiceImpl;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;

import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.regex.Pattern;

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

    private static final String JOB_DIRECTORY_MANIFEST_SERVICE_CS_BEAN = "checksummingJobDirectoryManifestService";
    private static final String JOB_DIRECTORY_MANIFEST_SERVICE_BEAN = "jobDirectoryManifestService";

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
     * @param jobArchivers                The ordered available {@link JobArchiver} implementations in the system
     * @param jobDirectoryManifestService the job directory manifest service
     * @return A {@link JobArchiveServiceImpl} instance
     */
    @Bean
    @ConditionalOnMissingBean(JobArchiveService.class)
    public JobArchiveService jobArchiveService(
        final List<JobArchiver> jobArchivers,
        @Qualifier(JOB_DIRECTORY_MANIFEST_SERVICE_CS_BEAN) final JobDirectoryManifestService jobDirectoryManifestService
    ) {
        return new JobArchiveServiceImpl(jobArchivers, jobDirectoryManifestService);
    }

    /**
     * Provide a {@link JobDirectoryManifestService} if no override is defined.
     * The manifest produced by this service include checksum for each entry.
     *
     * @param directoryManifestFactory the factory to produce the manifest if needed
     * @return a {@link JobDirectoryManifestService}
     */
    @Bean(name = JOB_DIRECTORY_MANIFEST_SERVICE_CS_BEAN)
    @ConditionalOnMissingBean(
        name = JOB_DIRECTORY_MANIFEST_SERVICE_CS_BEAN
    )
    public JobDirectoryManifestService checksummingJobDirectoryManifestService(
        final DirectoryManifest.Factory directoryManifestFactory
    ) {
        return jobDirectoryPath -> directoryManifestFactory.getDirectoryManifest(jobDirectoryPath, true);
    }

    /**
     * Provide a {@link JobDirectoryManifestService} if no override is defined.
     * The manifest produced by this service do not include checksum for each entry.
     *
     * @param directoryManifestFactory the factory to produce the manifest if needed
     * @return a {@link JobDirectoryManifestService}
     */
    @Bean(name = JOB_DIRECTORY_MANIFEST_SERVICE_BEAN)
    @Primary
    @ConditionalOnMissingBean(
        name = JOB_DIRECTORY_MANIFEST_SERVICE_BEAN
    )
    public JobDirectoryManifestService jobDirectoryManifestService(
        final DirectoryManifest.Factory directoryManifestFactory
    ) {
        return jobDirectoryPath -> directoryManifestFactory.getDirectoryManifest(jobDirectoryPath, false);
    }

    /**
     * Provide a {@link DirectoryManifest.Factory} if no override is defined.
     *
     * @param directoryManifestFilter the filter used during manifest creation
     * @return a directory manifest factory
     */
    @Bean
    @ConditionalOnMissingBean(DirectoryManifest.Factory.class)
    public DirectoryManifest.Factory directoryManifestFactory(
        final DirectoryManifest.Filter directoryManifestFilter
    ) {
        return new DirectoryManifest.Factory(directoryManifestFilter);
    }

    /**
     * Provide a {@link DirectoryManifest.Filter} if no override is defined.
     * This filter prunes subtrees of 'dependencies' directories (applications, clusters, commands).
     *
     * @return a directory manifest filter
     */
    @Bean
    @ConditionalOnMissingBean(DirectoryManifest.Filter.class)
    public DirectoryManifest.Filter directoryManifestFilter() {
        return new DirectoryManifest.Filter() {
            private static final String DEPENDENCIES_DIRECTORIES_PATTERN = ""
                + ".*/"
                + JobConstants.GENIE_PATH_VAR
                + "/.*/"
                + JobConstants.DEPENDENCY_FILE_PATH_PREFIX;
            private final Pattern dependenciesDirectoryPattern = Pattern.compile(DEPENDENCIES_DIRECTORIES_PATTERN);

            /**
             * {@inheritDoc}
             */
            @Override
            public boolean walkDirectory(final Path dirPath, final BasicFileAttributes attrs) {
                return !dependenciesDirectoryPattern.matcher(
                    dirPath.toAbsolutePath().normalize().toString()
                ).matches();
            }
        };
    }
}
