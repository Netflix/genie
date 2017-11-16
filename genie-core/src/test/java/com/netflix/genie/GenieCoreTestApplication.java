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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Files;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.Executor;
import org.apache.commons.exec.PumpStreamHandler;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;
import org.springframework.validation.beanvalidation.MethodValidationPostProcessor;

import javax.annotation.PreDestroy;
import javax.validation.Validator;
import java.io.File;
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

    private final File temporaryFolder = Files.createTempDir();

    /**
     * Clean up for beans and resources created in this test context.
     */
    @PreDestroy
    @SuppressWarnings("PMD.CollapsibleIfStatements") // Collapsing inner `if` statement is not equivalent to this.
    public void cleanUp() {
        if (this.temporaryFolder.exists()) {
            if (!this.temporaryFolder.delete()) {
                throw new RuntimeException("Temporary folder not deleted: " + this.temporaryFolder.toString());
            }
        }
    }

    /**
     * Setup bean validation.
     *
     * @return The bean validator
     */
    @Bean
    @ConditionalOnMissingBean(Validator.class)
    public Validator localValidatorFactoryBean() {
        return new LocalValidatorFactoryBean();
    }

    /**
     * Setup method parameter bean validation.
     *
     * @return The method validation processor
     */
    @Bean
    @ConditionalOnMissingBean(MethodValidationPostProcessor.class)
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
     * @throws IOException on error reading or creating the directory
     */
    @Bean
    @ConditionalOnMissingBean(value = Resource.class, name = "jobsDir")
    public Resource jobsDir() throws IOException {
        return new FileSystemResource(temporaryFolder.toString());
    }

    /**
     * Get a Jackson ObjectMapper.
     *
     * @return a new ObjectMapper with default options
     */
    @Bean
    @ConditionalOnMissingBean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }
}
