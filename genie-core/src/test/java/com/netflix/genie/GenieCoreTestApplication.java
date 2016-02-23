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

import com.github.springtestdbunit.bean.DatabaseConfigBean;
import com.github.springtestdbunit.bean.DatabaseDataSourceConnectionFactoryBean;
import org.dbunit.ext.hsqldb.HsqldbDataTypeFactory;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;
import org.springframework.validation.beanvalidation.MethodValidationPostProcessor;

import javax.sql.DataSource;
import javax.validation.Validator;

/**
 * Spring configuration class for integration tests.
 *
 * @author tgianos
 */
@Configuration
@EnableAutoConfiguration
@ComponentScan
public class  GenieCoreTestApplication {

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
    public String hostname() {
        return "localhost";
    }

    /**
     * Get the DBUnit configuration.
     *
     * @return The config bean
     */
    @Bean
     public DatabaseConfigBean dbUnitDatabaseConfig() {
        final DatabaseConfigBean dbConfig = new DatabaseConfigBean();
        dbConfig.setDatatypeFactory(new HsqldbDataTypeFactory());
        return dbConfig;
    }

    /**
     * Get the database connection factory bean.
     *
     * @param dataSource The data source to use
     * @return The database connection factory bean for dbunit.
     */
    @Bean
    public DatabaseDataSourceConnectionFactoryBean dbUnitDatabaseConnection(final DataSource dataSource) {
        final DatabaseDataSourceConnectionFactoryBean dbConnection
                = new DatabaseDataSourceConnectionFactoryBean(dataSource);
        dbConnection.setDatabaseConfig(dbUnitDatabaseConfig());
        return dbConnection;
    }

    /**
     * Setup a bean to provide the list of impl in the workflow.
     *
     * @return List of workflow impl.
     */
//    @Bean
//    // TODO Maybe return an empty list for testing? How is this used in testing.
//    public List<WorkflowTask> taskList() {
//        final List<WorkflowTask> taskList = new ArrayList<>();
//        taskList.add(new IntialSetupTask());
//        taskList.add(new ApplicationTask());
//        taskList.add(new CommandTask());
//        taskList.add(new ClusterTask());
//        taskList.add(new JobTask());
//        taskList.add(new JobKickoffTask());
//        return taskList;
//    }

//    /**
//     * Create an Application Task bean that processes all Applications needed for a job.
//     *
//     * @return An application task object
//     */
//    @Bean
//    public WorkflowTask applicationProcessorTask() {
//        return new ApplicationTask();
//    }
//
//    /**
//     * Setup a bean to provide the list of impl in the workflow.
//     *
//     * @return List of workflow impl.
//     */
//    @Bean
//    public List<WorkflowTask> taskList(
//    ) {
//        final List<WorkflowTask> taskList = new ArrayList<>();
//        taskList.add(applicationProcessorTask());
//
//        return taskList;
//    }
}
