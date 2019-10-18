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
package com.netflix.genie.web.spring.autoconfigure.apis.rest.v3.hateoas;

import com.netflix.genie.web.apis.rest.v3.hateoas.assemblers.ApplicationModelAssembler;
import com.netflix.genie.web.apis.rest.v3.hateoas.assemblers.ClusterModelAssembler;
import com.netflix.genie.web.apis.rest.v3.hateoas.assemblers.CommandModelAssembler;
import com.netflix.genie.web.apis.rest.v3.hateoas.assemblers.EntityModelAssemblers;
import com.netflix.genie.web.apis.rest.v3.hateoas.assemblers.JobExecutionModelAssembler;
import com.netflix.genie.web.apis.rest.v3.hateoas.assemblers.JobMetadataModelAssembler;
import com.netflix.genie.web.apis.rest.v3.hateoas.assemblers.JobModelAssembler;
import com.netflix.genie.web.apis.rest.v3.hateoas.assemblers.JobRequestModelAssembler;
import com.netflix.genie.web.apis.rest.v3.hateoas.assemblers.JobSearchResultModelAssembler;
import com.netflix.genie.web.apis.rest.v3.hateoas.assemblers.RootModelAssembler;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Spring auto configuration for HATEOAS module beans.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Configuration
public class HateoasAutoConfiguration {

    /**
     * Provide a resource assembler for application resources if none already exists.
     *
     * @return A {@link ApplicationModelAssembler} instance
     */
    @Bean
    @ConditionalOnMissingBean(ApplicationModelAssembler.class)
    public ApplicationModelAssembler applicationResourceAssembler() {
        return new ApplicationModelAssembler();
    }

    /**
     * Provide a resource assembler for cluster resources if none already exists.
     *
     * @return A {@link ClusterModelAssembler} instance
     */
    @Bean
    @ConditionalOnMissingBean(ClusterModelAssembler.class)
    public ClusterModelAssembler clusterResourceAssembler() {
        return new ClusterModelAssembler();
    }

    /**
     * Provide a resource assembler for command resources if none already exists.
     *
     * @return A {@link CommandModelAssembler} instance
     */
    @Bean
    @ConditionalOnMissingBean(CommandModelAssembler.class)
    public CommandModelAssembler commandResourceAssembler() {
        return new CommandModelAssembler();
    }

    /**
     * Provide a resource assembler for job execution resources if none already exists.
     *
     * @return A {@link JobExecutionModelAssembler} instance
     */
    @Bean
    @ConditionalOnMissingBean(JobExecutionModelAssembler.class)
    public JobExecutionModelAssembler jobExecutionResourceAssembler() {
        return new JobExecutionModelAssembler();
    }

    /**
     * Provide a resource assembler for job metadata resources if none already exists.
     *
     * @return A {@link JobMetadataModelAssembler} instance
     */
    @Bean
    @ConditionalOnMissingBean(JobMetadataModelAssembler.class)
    public JobMetadataModelAssembler jobMetadataResourceAssembler() {
        return new JobMetadataModelAssembler();
    }

    /**
     * Provide a resource assembler for job request resources if none already exists.
     *
     * @return A {@link JobRequestModelAssembler} instance
     */
    @Bean
    @ConditionalOnMissingBean(JobRequestModelAssembler.class)
    public JobRequestModelAssembler jobRequestResourceAssembler() {
        return new JobRequestModelAssembler();
    }

    /**
     * Provide a resource assembler for job resources if none already exists.
     *
     * @return A {@link JobModelAssembler} instance
     */
    @Bean
    @ConditionalOnMissingBean(JobModelAssembler.class)
    public JobModelAssembler jobResourceAssembler() {
        return new JobModelAssembler();
    }

    /**
     * Provide a resource assembler for job search result resources if none already exists.
     *
     * @return A {@link JobSearchResultModelAssembler} instance
     */
    @Bean
    @ConditionalOnMissingBean(JobSearchResultModelAssembler.class)
    public JobSearchResultModelAssembler jobSearchResultResourceAssembler() {
        return new JobSearchResultModelAssembler();
    }

    /**
     * Provide a resource assembler for the api root resource if none already exists.
     *
     * @return A {@link RootModelAssembler} instance
     */
    @Bean
    @ConditionalOnMissingBean(RootModelAssembler.class)
    public RootModelAssembler rootResourceAssembler() {
        return new RootModelAssembler();
    }

    /**
     * An encapsulation of all the V3 resource assemblers.
     *
     * @param applicationModelAssembler     The application assembler
     * @param clusterModelAssembler         The cluster assembler
     * @param commandModelAssembler         The command assembler
     * @param jobExecutionModelAssembler    The job execution assembler
     * @param jobMetadataModelAssembler     The job metadata assembler
     * @param jobRequestModelAssembler      The job request assembler
     * @param jobModelAssembler             The job assembler
     * @param jobSearchResultModelAssembler The job search result assembler
     * @param rootModelAssembler            The root assembler
     * @return A {@link EntityModelAssemblers} instance
     */
    @Bean
    @ConditionalOnMissingBean(EntityModelAssemblers.class)
    public EntityModelAssemblers resourceAssemblers(
        final ApplicationModelAssembler applicationModelAssembler,
        final ClusterModelAssembler clusterModelAssembler,
        final CommandModelAssembler commandModelAssembler,
        final JobExecutionModelAssembler jobExecutionModelAssembler,
        final JobMetadataModelAssembler jobMetadataModelAssembler,
        final JobRequestModelAssembler jobRequestModelAssembler,
        final JobModelAssembler jobModelAssembler,
        final JobSearchResultModelAssembler jobSearchResultModelAssembler,
        final RootModelAssembler rootModelAssembler
    ) {
        return new EntityModelAssemblers(
            applicationModelAssembler,
            clusterModelAssembler,
            commandModelAssembler,
            jobExecutionModelAssembler,
            jobMetadataModelAssembler,
            jobRequestModelAssembler,
            jobModelAssembler,
            jobSearchResultModelAssembler,
            rootModelAssembler
        );
    }
}
