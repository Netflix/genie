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

import com.google.common.collect.Maps;
import com.netflix.genie.core.jobs.workflow.impl.ApplicationTask;
import com.netflix.genie.core.jobs.workflow.impl.ClusterTask;
import com.netflix.genie.core.jobs.workflow.impl.CommandTask;
import com.netflix.genie.core.jobs.workflow.impl.IntialSetupTask;
import com.netflix.genie.core.jobs.workflow.impl.JobKickoffTask;
import com.netflix.genie.core.jobs.workflow.impl.JobTask;
import com.netflix.genie.core.jobs.workflow.WorkflowTask;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration;
import org.springframework.boot.autoconfigure.session.SessionAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;
import org.springframework.validation.beanvalidation.MethodValidationPostProcessor;

import javax.validation.Validator;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Main Genie Spring Configuration class.
 *
 * @author tgianos
 * @since 3.0.0
 */
@SpringBootApplication(exclude = {SessionAutoConfiguration.class, RedisAutoConfiguration.class})
public class GenieWeb {

    /**
     * Spring Boot Main.
     *
     * @param args Program arguments
     * @throws Exception For any failure during program execution
     */
    public static void main(final String[] args) throws Exception {
        final Map<String, Object> defaultProperties = Maps.newHashMap();
        defaultProperties.put("spring.config.location", "${user.home}/.genie/");
        final SpringApplication genie = new SpringApplication(GenieWeb.class);
        genie.setDefaultProperties(defaultProperties);
        genie.run(args);
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

    /**
     * Setup a bean to provide the list of impl in the workflow.
     *
     * @return List of workflow impl.
     */
    @Bean
    public List<WorkflowTask> taskList() {
        final List<WorkflowTask> taskList = new ArrayList<>();
        taskList.add(new IntialSetupTask());
        taskList.add(new ApplicationTask());
        taskList.add(new CommandTask());
        taskList.add(new ClusterTask());
        taskList.add(new JobTask());
        taskList.add(new JobKickoffTask());
        return taskList;
    }
}
