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
package com.netflix.genie;

import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.Executor;
import org.apache.commons.exec.PumpStreamHandler;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;
import org.springframework.validation.beanvalidation.MethodValidationPostProcessor;

import javax.validation.Validator;
import java.io.IOException;

/**
 * Spring configuration class for integration tests.
 *
 * @author tgianos
 */
@Configuration
@EnableAutoConfiguration
@ComponentScan
public class GenieCoreTestApplication {

    /**
     * Setup bean validation.
     *
     * @return The bean validator
     */
    @Bean
    @ConditionalOnMissingBean
    public Validator localValidatorFactoryBean() {
        return new LocalValidatorFactoryBean();
    }

    /**
     * Setup method parameter bean validation.
     *
     * @return The method validation processor
     */
    @Bean
    @ConditionalOnMissingBean
    public MethodValidationPostProcessor methodValidationPostProcessor() {
        return new MethodValidationPostProcessor();
    }

    /**
     * The hostname bean to use for integration tests.
     *
     * @return localhost always
     */
    @Bean
    public String hostName() {
        return "localhost";
    }

    /**
     * Get an {@link Executor} to use for executing processes from tasks.
     *
     * @return The executor to use
     */
    @Bean
    public Executor processExecutor() {
        final Executor executor = new DefaultExecutor();
        executor.setStreamHandler(new PumpStreamHandler(null, null));
        return executor;
    }

    /**
     * Get the jobs dir as a Spring Resource. Will create if it doesn't exist.
     *
     * @return The job dir as a resource
     * @throws IOException on error reading or creading the directory
     */
    @Bean
    @ConditionalOnMissingBean
    public Resource jobsDir(
    ) throws IOException {
        return new DefaultResourceLoader().getResource("/tmp");
    }
}
