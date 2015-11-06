/*
 *
 *  Copyright 2015 Netflix, Inc.
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
package com.netflix.genie.web;

import com.google.common.collect.Lists;
import com.netflix.genie.core.elasticsearch.repositories.EsJobRepository;
import com.netflix.genie.core.jpa.services.JpaJobPersistenceServiceImpl;
import com.netflix.genie.core.services.JobPersistenceService;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.orm.jpa.EntityScan;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.repository.config.EnableElasticsearchRepositories;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;
import org.springframework.validation.beanvalidation.MethodValidationPostProcessor;
import springfox.documentation.builders.PathSelectors;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.service.ApiInfo;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;
import com.netflix.genie.core.elasticsearch.services.EsJobPersistenceServiceImpl;

import javax.validation.Validator;
import java.util.List;

/**
 * Main Genie Spring Configuration class.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Configuration
@ComponentScan("com.netflix.genie")
@EnableAutoConfiguration
@EnableElasticsearchRepositories("com.netflix.genie.core.elasticsearch.repositories")
@EnableJpaRepositories("com.netflix.genie.core.jpa.repositories")
@EntityScan("com.netflix.genie.core.jpa.entities")
@EnableSwagger2
public class GenieWeb {

    /**
     * Elasticsearch bean for job persistence service.
     *
     * @param repository The elastic search repository
     * @param template   The elastic search template
     *
     * @return esbean The elasticsearch bean
     */
    @Bean
    public JobPersistenceService esbean(
            final EsJobRepository repository,
            final ElasticsearchTemplate template) {
        return new EsJobPersistenceServiceImpl(repository, template);
    }

    /**
     * JPA bean for job persistence service..
     *
     * @return jpabean The jpa bean.
     */
    @Bean
    public JobPersistenceService jpabean() {
        return new JpaJobPersistenceServiceImpl();
    }

    /**
     * Bean defining the order in which System would search for jobs from the Persistence layer.
     * If a job is retrieved from a layer, it would ignore the rest.
     *
     * @param esbean The elasticsearch bean reference
     * @param jpabean The jpabean reference
     *
     * @return list of beans implementing job persistence
     */
    @Bean
    @Qualifier("search")
    public List<JobPersistenceService> persistenceSearchPriorityOrder(
            final JobPersistenceService esbean,
            final JobPersistenceService jpabean
    ) {
        return Lists.newArrayList(
                esbean,
                jpabean);
    }

    /**
     * Bean defining the order in which System would save jobs int the Persistence layer.
     *
     * @param jpabean repository
     * @param esbean template
     *
     * @return list of beans implementing job persistence
     */
    @Bean
    @Qualifier("save")
    public List<JobPersistenceService> persistenceSavePriorityOrder(
            final JobPersistenceService jpabean,
            final JobPersistenceService esbean

    ) {
        return Lists.newArrayList(
                jpabean,
                esbean
        );
    }

    /**
     * Spring Boot Main.
     *
     * @param args Program arguments
     * @throws Exception For any failure during program execution
     */
    public static void main(final String[] args) throws Exception {
        SpringApplication.run(GenieWeb.class, args);
    }

    /**
     * Configure Spring Fox.
     *
     * @return The spring fox docket.
     */
    @Bean
    public Docket genieApi() {
        return new Docket(DocumentationType.SWAGGER_2)
                .apiInfo(
                        new ApiInfo(
                                "Genie REST API",
//                                "See our &lt;a href=&quot;http://netflix.github.io/genie&quot;&gt;GitHub Page"
//                                        + "&lt;/a&gt; for more documentation.&lt;br/&gt;Post any issues found &lt;"
//                                        + "a href=&quot;https://github.com/Netflix/genie/issues&quot;>here"
//                                        + "&lt;/a&gt;.&lt;br/&gt;",
                                "See our <a href=\"http://netflix.github.io/genie\">GitHub Page</a> for more "
                                        + "documentation.<br/>Post any issues found "
                                        + "<a href=\"https://github.com/Netflix/genie/issues\">here</a>.<br/>",
                                "3.0.0",
                                null,
                                "Netflix, Inc.",
                                "Apache 2.0",
                                "http://www.apache.org/licenses/LICENSE-2.0"
                        )
                )
                .select()
                .apis(RequestHandlerSelectors.basePackage("com.netflix.genie.web.controllers"))
                .paths(PathSelectors.any())
                .build()
                .pathMapping("/")
                .useDefaultResponseMessages(false);
    }

    /**
     * Setup bean validation.
     *
     * @return The bean validator
     */
    @Bean
    public Validator localValidatorFactoryBean() {
        return new LocalValidatorFactoryBean();
    }

    /**
     * Setup method parameter bean validation.
     *
     * @return The method validation processor
     */
    @Bean
    public MethodValidationPostProcessor methodValidationPostProcessor() {
        return new MethodValidationPostProcessor();
    }
}
