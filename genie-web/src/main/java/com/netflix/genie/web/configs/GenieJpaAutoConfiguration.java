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
package com.netflix.genie.web.configs;

import com.netflix.genie.web.jpa.repositories.JpaAgentConnectionRepository;
import com.netflix.genie.web.jpa.repositories.JpaApplicationRepository;
import com.netflix.genie.web.jpa.repositories.JpaClusterRepository;
import com.netflix.genie.web.jpa.repositories.JpaCommandRepository;
import com.netflix.genie.web.jpa.repositories.JpaFileRepository;
import com.netflix.genie.web.jpa.repositories.JpaJobRepository;
import com.netflix.genie.web.jpa.repositories.JpaTagRepository;
import com.netflix.genie.web.jpa.services.JpaAgentConnectionPersistenceServiceImpl;
import com.netflix.genie.web.jpa.services.JpaApplicationPersistenceServiceImpl;
import com.netflix.genie.web.jpa.services.JpaClusterPersistenceServiceImpl;
import com.netflix.genie.web.jpa.services.JpaCommandPersistenceServiceImpl;
import com.netflix.genie.web.jpa.services.JpaFilePersistenceService;
import com.netflix.genie.web.jpa.services.JpaFilePersistenceServiceImpl;
import com.netflix.genie.web.jpa.services.JpaJobPersistenceServiceImpl;
import com.netflix.genie.web.jpa.services.JpaJobSearchServiceImpl;
import com.netflix.genie.web.jpa.services.JpaTagPersistenceService;
import com.netflix.genie.web.jpa.services.JpaTagPersistenceServiceImpl;
import com.netflix.genie.web.services.AgentConnectionPersistenceService;
import com.netflix.genie.web.services.ApplicationPersistenceService;
import com.netflix.genie.web.services.ClusterPersistenceService;
import com.netflix.genie.web.services.CommandPersistenceService;
import com.netflix.genie.web.services.FilePersistenceService;
import com.netflix.genie.web.services.JobPersistenceService;
import com.netflix.genie.web.services.JobSearchService;
import com.netflix.genie.web.services.TagPersistenceService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

/**
 * Auto configuration of JPA related services and beans for Genie.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Configuration
// TODO: Create marker class for base classes scanning for compile time check
@EnableJpaRepositories("com.netflix.genie.web.jpa.repositories")
@EntityScan("com.netflix.genie.web.jpa.entities")
public class GenieJpaAutoConfiguration {

    /**
     * The JPA based implementation of the {@link ApplicationPersistenceService} interface.
     *
     * @param tagPersistenceService  The {@link JpaTagPersistenceService} to use
     * @param filePersistenceService The {@link JpaFilePersistenceService} to use
     * @param applicationRepository  The {@link JpaApplicationRepository} to use
     * @param clusterRepository      The {@link JpaClusterRepository} to use
     * @param commandRepository      The {@link JpaCommandRepository} to use
     * @return A {@link JpaApplicationPersistenceServiceImpl} instance.
     */
    @Bean
    @ConditionalOnMissingBean(ApplicationPersistenceService.class)
    public JpaApplicationPersistenceServiceImpl applicationPersistenceService(
        final JpaTagPersistenceService tagPersistenceService,
        final JpaFilePersistenceService filePersistenceService,
        final JpaApplicationRepository applicationRepository,
        final JpaClusterRepository clusterRepository,
        final JpaCommandRepository commandRepository
    ) {
        return new JpaApplicationPersistenceServiceImpl(
            tagPersistenceService,
            filePersistenceService,
            applicationRepository,
            clusterRepository,
            commandRepository
        );
    }

    /**
     * The JPA implementation of the {@link ClusterPersistenceService} interface.
     *
     * @param tagPersistenceService  The {@link JpaTagPersistenceService} to use
     * @param filePersistenceService The {@link JpaFilePersistenceService} to use
     * @param applicationRepository  The {@link JpaApplicationRepository} to use
     * @param clusterRepository      The {@link JpaClusterRepository} to use
     * @param commandRepository      The {@link JpaCommandRepository} to use
     * @return A {@link JpaClusterPersistenceServiceImpl} instance
     */
    @Bean
    @ConditionalOnMissingBean(ClusterPersistenceService.class)
    public JpaClusterPersistenceServiceImpl clusterPersistenceService(
        final JpaTagPersistenceService tagPersistenceService,
        final JpaFilePersistenceService filePersistenceService,
        final JpaApplicationRepository applicationRepository,
        final JpaClusterRepository clusterRepository,
        final JpaCommandRepository commandRepository
    ) {
        return new JpaClusterPersistenceServiceImpl(
            tagPersistenceService,
            filePersistenceService,
            applicationRepository,
            clusterRepository,
            commandRepository
        );
    }

    /**
     * The JPA implementation of the {@link CommandPersistenceService} interface.
     *
     * @param tagPersistenceService  The {@link JpaTagPersistenceService} to use
     * @param filePersistenceService The {@link JpaFilePersistenceService} to use
     * @param applicationRepository  The {@link JpaApplicationRepository} to use
     * @param clusterRepository      The {@link JpaClusterRepository} to use
     * @param commandRepository      The {@link JpaCommandRepository} to use
     * @return A {@link JpaCommandPersistenceServiceImpl} instance
     */
    @Bean
    @ConditionalOnMissingBean(CommandPersistenceService.class)
    public JpaCommandPersistenceServiceImpl commandPersistenceService(
        final JpaTagPersistenceService tagPersistenceService,
        final JpaFilePersistenceService filePersistenceService,
        final JpaApplicationRepository applicationRepository,
        final JpaClusterRepository clusterRepository,
        final JpaCommandRepository commandRepository
    ) {
        return new JpaCommandPersistenceServiceImpl(
            tagPersistenceService,
            filePersistenceService,
            applicationRepository,
            clusterRepository,
            commandRepository
        );
    }

    /**
     * A JPA implementation of the {@link FilePersistenceService} interface. Also implements
     * {@link JpaFilePersistenceService}.
     *
     * @param fileRepository The repository to use to perform CRUD operations on files
     * @return A {@link JpaFilePersistenceServiceImpl} instance
     */
    @Bean
    @ConditionalOnMissingBean(FilePersistenceService.class)
    public JpaFilePersistenceServiceImpl filePersistenceService(final JpaFileRepository fileRepository) {
        return new JpaFilePersistenceServiceImpl(fileRepository);
    }

    /**
     * A JPA implementation of the {@link TagPersistenceService} interface. Also implements
     * {@link JpaTagPersistenceService}.
     *
     * @param tagRepository The repository to use to perform CRUD operations on tags
     * @return A {@link JpaTagPersistenceServiceImpl} instance
     */
    @Bean
    @ConditionalOnMissingBean(TagPersistenceService.class)
    public JpaTagPersistenceServiceImpl tagPersistenceService(final JpaTagRepository tagRepository) {
        return new JpaTagPersistenceServiceImpl(tagRepository);
    }

    /**
     * JPA implementation of the {@link JobPersistenceService} interface.
     *
     * @param tagPersistenceService  The {@link JpaTagPersistenceService} to use
     * @param filePersistenceService The {@link JpaFilePersistenceService} to use
     * @param applicationRepository  The {@link JpaApplicationRepository} to use
     * @param clusterRepository      The {@link JpaClusterRepository} to use
     * @param commandRepository      The {@link JpaCommandRepository} to use
     * @param jobRepository          The {@link JpaJobRepository} to use
     * @return Instance of {@link JpaJobPersistenceServiceImpl}
     */
    @Bean
    @ConditionalOnMissingBean(JobPersistenceService.class)
    public JpaJobPersistenceServiceImpl jobPersistenceService(
        final JpaTagPersistenceService tagPersistenceService,
        final JpaFilePersistenceService filePersistenceService,
        final JpaApplicationRepository applicationRepository,
        final JpaClusterRepository clusterRepository,
        final JpaCommandRepository commandRepository,
        final JpaJobRepository jobRepository
    ) {
        return new JpaJobPersistenceServiceImpl(
            tagPersistenceService,
            filePersistenceService,
            applicationRepository,
            clusterRepository,
            commandRepository,
            jobRepository
        );
    }

    /**
     * Get a JPA implementation of the {@link JobSearchService} if one didn't already exist.
     *
     * @param jobRepository     The repository to use for job entities
     * @param clusterRepository The repository to use for cluster entities
     * @param commandRepository The repository to use for command entities
     * @return A {@link JpaJobSearchServiceImpl} instance
     */
    @Bean
    @ConditionalOnMissingBean(JobSearchService.class)
    public JpaJobSearchServiceImpl jobSearchService(
        final JpaJobRepository jobRepository,
        final JpaClusterRepository clusterRepository,
        final JpaCommandRepository commandRepository
    ) {
        return new JpaJobSearchServiceImpl(jobRepository, clusterRepository, commandRepository);
    }


    /**
     * A JPA implementation of the {@link AgentConnectionPersistenceService} interface.
     *
     * @param jpaAgentConnectionRepository The repository to use for agent connection entities
     * @return A {@link JpaAgentConnectionPersistenceServiceImpl} instance
     */
    @Bean
    @ConditionalOnMissingBean(AgentConnectionPersistenceService.class)
    public JpaAgentConnectionPersistenceServiceImpl agentConnectionPersistenceService(
        final JpaAgentConnectionRepository jpaAgentConnectionRepository
    ) {
        return new JpaAgentConnectionPersistenceServiceImpl(jpaAgentConnectionRepository);
    }
}
