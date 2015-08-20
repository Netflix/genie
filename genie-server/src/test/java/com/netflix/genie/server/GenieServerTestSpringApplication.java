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
package com.netflix.genie.server;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.orm.jpa.EntityScan;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.elasticsearch.repository.config.EnableElasticsearchRepositories;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;
import org.springframework.validation.beanvalidation.MethodValidationPostProcessor;

import javax.validation.Validator;

/**
 * Spring configuration class for integration tests.
 *
 * @author tgianos
 */
@Configuration
@EnableAutoConfiguration
@ComponentScan("com.netflix.genie")
//@ComponentScan(
//        {
//                "com.netflix.genie.server.jobmanager",
//                "com.netflix.genie.server.metrics",
//                "com.netflix.genie.server.services",
//                "com.netflix.genie.server.startup",
//                "com.netflix.genie.server.util"
//        }
//)
@EnableJpaRepositories("com.netflix.genie.server.repository.jpa")
@EnableElasticsearchRepositories("com.netflix.genie.server.repository.elasticsearch")
@EntityScan("com.netflix.genie.common.model")
@EnableTransactionManagement
@EnableRetry
public class GenieServerTestSpringApplication {

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
