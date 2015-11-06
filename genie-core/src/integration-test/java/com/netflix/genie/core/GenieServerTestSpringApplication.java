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
package com.netflix.genie.core;

import com.github.springtestdbunit.bean.DatabaseConfigBean;
import com.github.springtestdbunit.bean.DatabaseDataSourceConnectionFactoryBean;
import com.google.common.collect.Lists;
import com.netflix.genie.core.elasticsearch.repositories.EsJobRepository;
import com.netflix.genie.core.elasticsearch.services.EsJobPersistenceServiceImpl;
import com.netflix.genie.core.jpa.services.JpaJobPersistenceServiceImpl;
import com.netflix.genie.core.services.JobPersistenceService;
import org.dbunit.ext.hsqldb.HsqldbDataTypeFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.orm.jpa.EntityScan;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.repository.config.EnableElasticsearchRepositories;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;
import org.springframework.validation.beanvalidation.MethodValidationPostProcessor;

import javax.sql.DataSource;
import javax.validation.Validator;
import java.util.List;

/**
 * Spring configuration class for integration tests.
 *
 * @author tgianos
 */
@Configuration
@EnableAutoConfiguration
@ComponentScan("com.netflix.genie")
@EnableJpaRepositories("com.netflix.genie.core.jpa.repositories")
@EnableElasticsearchRepositories("com.netflix.genie.core.elasticsearch.repositories")
@EntityScan("com.netflix.genie.core.jpa.entities")
@EnableTransactionManagement
@EnableRetry
public class GenieServerTestSpringApplication {

    /**
     * Description.
     *
     * @param repository repo
     * @param template template
     * @return foo
     */
    @Bean
    public JobPersistenceService esbean(
            final EsJobRepository repository,
            final ElasticsearchTemplate template) {
        return new EsJobPersistenceServiceImpl(repository, template);
    }

    /**
     * Description.
     *
     * @return foo
     */
    @Bean
    public JobPersistenceService jpabean() {
        return new JpaJobPersistenceServiceImpl();
    }

    /**
     * Description.
     *
     * @param esbean repo
     * @param jpabean template
     * @return something
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
     * Description.
     *
     * @param jpabean repository
     * @param esbean template
     * @return something
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
}
