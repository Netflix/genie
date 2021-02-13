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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.netflix.genie.common.internal.dtos.DirectoryManifest;
import com.netflix.genie.common.internal.properties.RegexDirectoryManifestProperties;
import com.netflix.genie.common.internal.services.JobArchiveService;
import com.netflix.genie.common.internal.services.JobArchiver;
import com.netflix.genie.common.internal.services.JobDirectoryManifestCreatorService;
import com.netflix.genie.common.internal.services.impl.FileSystemJobArchiverImpl;
import com.netflix.genie.common.internal.services.impl.JobArchiveServiceImpl;
import com.netflix.genie.common.internal.services.impl.JobDirectoryManifestCreatorServiceImpl;
import com.netflix.genie.common.internal.util.PropertiesMapCache;
import com.netflix.genie.common.internal.util.RegexDirectoryManifestFilter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.core.env.Environment;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Auto configuration of any services that are common to both the agent and the server.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Configuration
@EnableConfigurationProperties(
    {
        RegexDirectoryManifestProperties.class
    }
)
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
     * @param jobArchivers             The ordered available {@link JobArchiver} implementations in the system
     * @param directoryManifestFactory the job directory manifest factory
     * @return A {@link JobArchiveServiceImpl} instance
     */
    @Bean
    @ConditionalOnMissingBean(JobArchiveService.class)
    public JobArchiveService jobArchiveService(
        final List<JobArchiver> jobArchivers,
        final DirectoryManifest.Factory directoryManifestFactory
    ) {
        return new JobArchiveServiceImpl(jobArchivers, directoryManifestFactory);
    }

    /**
     * Provide a {@link JobDirectoryManifestCreatorService} if no override is defined.
     * The manifest produced by this service do not include checksum for entries and caches manifests recently created.
     *
     * @param directoryManifestFactory the factory to produce the manifest if needed
     * @param cache                    the cache to use
     * @return a {@link JobDirectoryManifestCreatorService}
     */
    @Bean
    @ConditionalOnMissingBean(JobDirectoryManifestCreatorService.class)
    public JobDirectoryManifestCreatorServiceImpl jobDirectoryManifestCreatorService(
        final DirectoryManifest.Factory directoryManifestFactory,
        @Qualifier("jobDirectoryManifestCache") final Cache<Path, DirectoryManifest> cache
    ) {
        return new JobDirectoryManifestCreatorServiceImpl(directoryManifestFactory, cache, false);
    }

    /**
     * Provide a {@code Cache<Path, DirectoryManifest>} named "jobDirectoryManifestCache" if no override is defined.
     *
     * @return a {@link Cache}
     */
    @Bean(name = "jobDirectoryManifestCache")
    @ConditionalOnMissingBean(name = "jobDirectoryManifestCache")
    public Cache<Path, DirectoryManifest> jobDirectoryManifestCache() {
        // TODO hardcoded configuration values
        return Caffeine.newBuilder()
            .maximumSize(100)
            .expireAfterWrite(30, TimeUnit.SECONDS)
            .build();
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
     * Provide a {@link DirectoryManifest.Filter} that filters files and/or prunes directories based on a set of
     * regular expression patterns provided via properties.
     *
     * @param properties the properties
     * @return a directory manifest filter
     */
    @Bean
    @ConditionalOnMissingBean(DirectoryManifest.Filter.class)
    public DirectoryManifest.Filter directoryManifestFilter(final RegexDirectoryManifestProperties properties) {
        return new RegexDirectoryManifestFilter(properties);
    }

    /**
     * Provide a {@link PropertiesMapCache.Factory} if no override is defined.
     *
     * @param environment the environment
     * @return a properties map cache factory
     */
    @Bean
    @ConditionalOnMissingBean(PropertiesMapCache.Factory.class)
    public PropertiesMapCache.Factory propertiesMapCacheFactory(final Environment environment) {
        return new PropertiesMapCache.Factory(environment);
    }
}
