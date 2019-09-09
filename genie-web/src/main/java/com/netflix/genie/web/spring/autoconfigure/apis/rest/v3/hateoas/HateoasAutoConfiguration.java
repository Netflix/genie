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

import com.netflix.genie.web.apis.rest.v3.hateoas.assemblers.ApplicationResourceAssembler;
import com.netflix.genie.web.apis.rest.v3.hateoas.assemblers.ClusterResourceAssembler;
import com.netflix.genie.web.apis.rest.v3.hateoas.assemblers.CommandResourceAssembler;
import com.netflix.genie.web.apis.rest.v3.hateoas.assemblers.JobExecutionResourceAssembler;
import com.netflix.genie.web.apis.rest.v3.hateoas.assemblers.JobMetadataResourceAssembler;
import com.netflix.genie.web.apis.rest.v3.hateoas.assemblers.JobRequestResourceAssembler;
import com.netflix.genie.web.apis.rest.v3.hateoas.assemblers.JobResourceAssembler;
import com.netflix.genie.web.apis.rest.v3.hateoas.assemblers.JobSearchResultResourceAssembler;
import com.netflix.genie.web.apis.rest.v3.hateoas.assemblers.RootResourceAssembler;
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
     * @return A {@link ApplicationResourceAssembler} instance
     */
    @Bean
    @ConditionalOnMissingBean(ApplicationResourceAssembler.class)
    public ApplicationResourceAssembler applicationResourceAssembler() {
        return new ApplicationResourceAssembler();
    }

    /**
     * Provide a resource assembler for cluster resources if none already exists.
     *
     * @return A {@link ClusterResourceAssembler} instance
     */
    @Bean
    @ConditionalOnMissingBean(ClusterResourceAssembler.class)
    public ClusterResourceAssembler clusterResourceAssembler() {
        return new ClusterResourceAssembler();
    }

    /**
     * Provide a resource assembler for command resources if none already exists.
     *
     * @return A {@link CommandResourceAssembler} instance
     */
    @Bean
    @ConditionalOnMissingBean(CommandResourceAssembler.class)
    public CommandResourceAssembler commandResourceAssembler() {
        return new CommandResourceAssembler();
    }

    /**
     * Provide a resource assembler for job execution resources if none already exists.
     *
     * @return A {@link JobExecutionResourceAssembler} instance
     */
    @Bean
    @ConditionalOnMissingBean(JobExecutionResourceAssembler.class)
    public JobExecutionResourceAssembler jobExecutionResourceAssembler() {
        return new JobExecutionResourceAssembler();
    }

    /**
     * Provide a resource assembler for job metadata resources if none already exists.
     *
     * @return A {@link JobMetadataResourceAssembler} instance
     */
    @Bean
    @ConditionalOnMissingBean(JobMetadataResourceAssembler.class)
    public JobMetadataResourceAssembler jobMetadataResourceAssembler() {
        return new JobMetadataResourceAssembler();
    }

    /**
     * Provide a resource assembler for job request resources if none already exists.
     *
     * @return A {@link JobRequestResourceAssembler} instance
     */
    @Bean
    @ConditionalOnMissingBean(JobRequestResourceAssembler.class)
    public JobRequestResourceAssembler jobRequestResourceAssembler() {
        return new JobRequestResourceAssembler();
    }

    /**
     * Provide a resource assembler for job resources if none already exists.
     *
     * @return A {@link JobResourceAssembler} instance
     */
    @Bean
    @ConditionalOnMissingBean(JobResourceAssembler.class)
    public JobResourceAssembler jobResourceAssembler() {
        return new JobResourceAssembler();
    }

    /**
     * Provide a resource assembler for job search result resources if none already exists.
     *
     * @return A {@link JobSearchResultResourceAssembler} instance
     */
    @Bean
    @ConditionalOnMissingBean(JobSearchResultResourceAssembler.class)
    public JobSearchResultResourceAssembler jobSearchResultResourceAssembler() {
        return new JobSearchResultResourceAssembler();
    }

    /**
     * Provide a resource assembler for the api root resource if none already exists.
     *
     * @return A {@link RootResourceAssembler} instance
     */
    @Bean
    @ConditionalOnMissingBean
    public RootResourceAssembler rootResourceAssembler() {
        return new RootResourceAssembler();
    }
}
