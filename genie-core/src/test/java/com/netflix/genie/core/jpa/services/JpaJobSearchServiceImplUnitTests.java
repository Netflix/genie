/*
 *
 *  Copyright 2016 Netflix, Inc.
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
package com.netflix.genie.core.jpa.services;

import com.netflix.genie.common.dto.Job;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.core.jpa.entities.JobEntity;
import com.netflix.genie.core.jpa.repositories.JpaJobExecutionRepository;
import com.netflix.genie.core.jpa.repositories.JpaJobRepository;
import com.netflix.genie.test.categories.UnitTest;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import java.util.UUID;

/**
 * Unit tests for JpaJobSearchServiceImpl.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class JpaJobSearchServiceImplUnitTests {

    private JpaJobRepository jobRepository;
    private JpaJobSearchServiceImpl service;

    /**
     * Setup for the tests.
     */
    @Before
    public void setup() {
        this.jobRepository = Mockito.mock(JpaJobRepository.class);
        this.service = new JpaJobSearchServiceImpl(this.jobRepository, Mockito.mock(JpaJobExecutionRepository.class));
    }

    /**
     * Test the getJob method.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void cantGetJobIfDoesNotExist() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jobRepository.findOne(Mockito.eq(id))).thenReturn(null);
        this.service.getJob(id);
    }

    /**
     * Test the get job method to verify that the id sent is used to fetch from persistence service.
     *
     * @throws GenieException If there is any problem
     */
    @Test
    public void canGetJob() throws GenieException {
        final JobEntity jobEntity = Mockito.mock(JobEntity.class);
        final Job job = Mockito.mock(Job.class);
        Mockito.when(jobEntity.getDTO()).thenReturn(job);
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jobRepository.findOne(id)).thenReturn(jobEntity);
        final Job returnedJob = this.service.getJob(id);
        Mockito.verify(this.jobRepository, Mockito.times(1)).findOne(id);
        Assert.assertThat(returnedJob, Matchers.is(job));
    }
}
