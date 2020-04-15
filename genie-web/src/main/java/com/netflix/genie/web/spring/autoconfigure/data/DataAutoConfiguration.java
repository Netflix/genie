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
package com.netflix.genie.web.spring.autoconfigure.data;

import com.netflix.genie.web.data.repositories.jpa.JpaAgentConnectionRepository;
import com.netflix.genie.web.data.repositories.jpa.JpaApplicationRepository;
import com.netflix.genie.web.data.repositories.jpa.JpaClusterRepository;
import com.netflix.genie.web.data.repositories.jpa.JpaCommandRepository;
import com.netflix.genie.web.data.repositories.jpa.JpaCriterionRepository;
import com.netflix.genie.web.data.repositories.jpa.JpaFileRepository;
import com.netflix.genie.web.data.repositories.jpa.JpaJobRepository;
import com.netflix.genie.web.data.repositories.jpa.JpaRepositories;
import com.netflix.genie.web.data.repositories.jpa.JpaTagRepository;
import com.netflix.genie.web.data.services.AgentConnectionPersistenceService;
import com.netflix.genie.web.data.services.ApplicationPersistenceService;
import com.netflix.genie.web.data.services.ClusterPersistenceService;
import com.netflix.genie.web.data.services.CommandPersistenceService;
import com.netflix.genie.web.data.services.DataServices;
import com.netflix.genie.web.data.services.FilePersistenceService;
import com.netflix.genie.web.data.services.JobPersistenceService;
import com.netflix.genie.web.data.services.JobSearchService;
import com.netflix.genie.web.data.services.TagPersistenceService;
import com.netflix.genie.web.data.services.jpa.JpaAgentConnectionPersistenceServiceImpl;
import com.netflix.genie.web.data.services.jpa.JpaApplicationPersistenceServiceImpl;
import com.netflix.genie.web.data.services.jpa.JpaClusterPersistenceServiceImpl;
import com.netflix.genie.web.data.services.jpa.JpaCommandPersistenceServiceImpl;
import com.netflix.genie.web.data.services.jpa.JpaFilePersistenceService;
import com.netflix.genie.web.data.services.jpa.JpaFilePersistenceServiceImpl;
import com.netflix.genie.web.data.services.jpa.JpaJobPersistenceServiceImpl;
import com.netflix.genie.web.data.services.jpa.JpaJobSearchServiceImpl;
import com.netflix.genie.web.data.services.jpa.JpaTagPersistenceService;
import com.netflix.genie.web.data.services.jpa.JpaTagPersistenceServiceImpl;
import com.netflix.genie.web.services.AttachmentService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

/**
 * Default auto configuration of data related services and beans for Genie.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Configuration
// TODO: Create marker class for base classes scanning for compile time check
@EnableJpaRepositories("com.netflix.genie.web.data.repositories")
@EntityScan("com.netflix.genie.web.data.entities")
public class DataAutoConfiguration {

    /**
     * The JPA based implementation of the {@link ApplicationPersistenceService} interface.
     *
     * @param tagPersistenceService  The {@link JpaTagPersistenceService} to use
     * @param filePersistenceService The {@link JpaFilePersistenceService} to use
     * @param jpaRepositories        The {@link JpaRepositories} containing all the repositories to use
     * @return A {@link JpaApplicationPersistenceServiceImpl} instance.
     */
    @Bean
    @ConditionalOnMissingBean(ApplicationPersistenceService.class)
    public JpaApplicationPersistenceServiceImpl applicationPersistenceService(
        final JpaTagPersistenceService tagPersistenceService,
        final JpaFilePersistenceService filePersistenceService,
        final JpaRepositories jpaRepositories
    ) {
        return new JpaApplicationPersistenceServiceImpl(tagPersistenceService, filePersistenceService, jpaRepositories);
    }

    /**
     * The JPA implementation of the {@link ClusterPersistenceService} interface.
     *
     * @param tagPersistenceService  The {@link JpaTagPersistenceService} to use
     * @param filePersistenceService The {@link JpaFilePersistenceService} to use
     * @param jpaRepositories        The {@link JpaRepositories} containing all the repositories to use
     * @return A {@link JpaClusterPersistenceServiceImpl} instance
     */
    @Bean
    @ConditionalOnMissingBean(ClusterPersistenceService.class)
    public JpaClusterPersistenceServiceImpl clusterPersistenceService(
        final JpaTagPersistenceService tagPersistenceService,
        final JpaFilePersistenceService filePersistenceService,
        final JpaRepositories jpaRepositories
    ) {
        return new JpaClusterPersistenceServiceImpl(tagPersistenceService, filePersistenceService, jpaRepositories);
    }

    /**
     * The JPA implementation of the {@link CommandPersistenceService} interface.
     *
     * @param tagPersistenceService  The {@link JpaTagPersistenceService} to use
     * @param filePersistenceService The {@link JpaFilePersistenceService} to use
     * @param jpaRepositories        The {@link JpaRepositories} containing all the repositories to use
     * @return A {@link JpaCommandPersistenceServiceImpl} instance
     */
    @Bean
    @ConditionalOnMissingBean(CommandPersistenceService.class)
    public JpaCommandPersistenceServiceImpl commandPersistenceService(
        final JpaTagPersistenceService tagPersistenceService,
        final JpaFilePersistenceService filePersistenceService,
        final JpaRepositories jpaRepositories
    ) {
        return new JpaCommandPersistenceServiceImpl(tagPersistenceService, filePersistenceService, jpaRepositories);
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
     * @param jpaRepositories        The {@link JpaRepositories} containing all the repositories to use
     * @param attachmentService      The {@link AttachmentService} implementation to use to store attachments for a job
     *                               before they are converted to dependencies
     * @return Instance of {@link JpaJobPersistenceServiceImpl}
     */
    @Bean
    @ConditionalOnMissingBean(JobPersistenceService.class)
    public JpaJobPersistenceServiceImpl jobPersistenceService(
        final JpaTagPersistenceService tagPersistenceService,
        final JpaFilePersistenceService filePersistenceService,
        final JpaRepositories jpaRepositories,
        final AttachmentService attachmentService
    ) {
        return new JpaJobPersistenceServiceImpl(
            tagPersistenceService,
            filePersistenceService,
            jpaRepositories,
            attachmentService
        );
    }

    /**
     * Get a JPA implementation of the {@link JobSearchService} if one didn't already exist.
     *
     * @param jpaRepositories The {@link JpaRepositories} containing all the repositories to use
     * @return A {@link JpaJobSearchServiceImpl} instance
     */
    @Bean
    @ConditionalOnMissingBean(JobSearchService.class)
    public JpaJobSearchServiceImpl jobSearchService(final JpaRepositories jpaRepositories) {
        return new JpaJobSearchServiceImpl(jpaRepositories);
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

    /**
     * Provide a {@link DataServices} instance if one isn't already in the context.
     *
     * @param agentConnectionPersistenceService The {@link AgentConnectionPersistenceService} implementation to use
     * @param applicationPersistenceService     The {@link ApplicationPersistenceService} implementation to use
     * @param clusterPersistenceService         The {@link ClusterPersistenceService} implementation to use
     * @param commandPersistenceService         The {@link CommandPersistenceService} implementation to use
     * @param filePersistenceService            The {@link FilePersistenceService} implementation to use
     * @param jobPersistenceService             The {@link JobPersistenceService} implementation to use
     * @param jobSearchService                  The {@link JobSearchService} implementation to use
     * @param tagPersistenceService             The {@link TagPersistenceService} implementation to use
     * @return A {@link DataServices} instance
     */
    @Bean
    @ConditionalOnMissingBean(DataServices.class)
    public DataServices genieDataServices(
        final AgentConnectionPersistenceService agentConnectionPersistenceService,
        final ApplicationPersistenceService applicationPersistenceService,
        final ClusterPersistenceService clusterPersistenceService,
        final CommandPersistenceService commandPersistenceService,
        final FilePersistenceService filePersistenceService,
        final JobPersistenceService jobPersistenceService,
        final JobSearchService jobSearchService,
        final TagPersistenceService tagPersistenceService
    ) {
        return new DataServices(
            agentConnectionPersistenceService,
            applicationPersistenceService,
            clusterPersistenceService,
            commandPersistenceService,
            filePersistenceService,
            jobPersistenceService,
            jobSearchService,
            tagPersistenceService
        );
    }

    /**
     * Provide a {@link JpaRepositories} container instance if one wasn't already provided.
     *
     * @param agentConnectionRepository The {@link JpaAgentConnectionRepository} instance
     * @param applicationRepository     The {@link JpaApplicationRepository} instance
     * @param clusterRepository         The {@link JpaClusterRepository} instance
     * @param commandRepository         The {@link JpaCommandRepository} instance
     * @param criterionRepository       The {@link JpaCriterionRepository} instance
     * @param fileRepository            The {@link JpaFileRepository} instance
     * @param jobRepository             The {@link JpaJobRepository} instance
     * @param tagRepository             The {@link JpaTagRepository} instance
     * @return A new {@link JpaRepositories} instance to simplify passing around all repositories
     */
    @Bean
    @ConditionalOnMissingBean(JpaRepositories.class)
    public JpaRepositories genieJpaRepositories(
        final JpaAgentConnectionRepository agentConnectionRepository,
        final JpaApplicationRepository applicationRepository,
        final JpaClusterRepository clusterRepository,
        final JpaCommandRepository commandRepository,
        final JpaCriterionRepository criterionRepository,
        final JpaFileRepository fileRepository,
        final JpaJobRepository jobRepository,
        final JpaTagRepository tagRepository
    ) {
        return new JpaRepositories(
            agentConnectionRepository,
            applicationRepository,
            clusterRepository,
            commandRepository,
            criterionRepository,
            fileRepository,
            jobRepository,
            tagRepository
        );
    }
}
