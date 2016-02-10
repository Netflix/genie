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
package com.netflix.genie.core.services.impl;

import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.core.services.JobCoordinatorService;
import com.netflix.genie.core.services.JobPersistenceService;
import com.netflix.genie.core.services.JobSearchService;
import com.netflix.genie.core.services.JobSubmitterService;
import com.netflix.genie.test.categories.UnitTest;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import java.util.UUID;

/**
 * Unit tests for JobServiceImpl.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class JobCoordinatorServiceImplUnitTests {

    private JobCoordinatorService jobService;
    private JobPersistenceService jobPersistenceService;
    private JobSearchService jobSearchService;
    private JobSubmitterService jobSubmitterService;

    /**
     * Setup for the tests.
     */
    @Before
    public void setup() {
        this.jobPersistenceService = Mockito.mock(JobPersistenceService.class);
        this.jobSearchService = Mockito.mock(JobSearchService.class);
        this.jobSubmitterService = Mockito.mock(JobSubmitterService.class);

        this.jobService = new JobCoordinatorServiceImpl(
            this.jobPersistenceService,
            this.jobSearchService,
            this.jobSubmitterService
        );
    }

    /**
     * Make sure if a job execution isn't found it returns a GenieNotFound exception.
     *
     * @throws GenieException for any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void cantGetJobHostIfNoJobExecution() throws GenieException {
        final String jobId = UUID.randomUUID().toString();
        Mockito.when(this.jobPersistenceService.getJobExecution(Mockito.eq(jobId))).thenReturn(null);
        this.jobService.getJobHost(jobId);
    }

    /**
     * Make sure that if the job execution exists we return a valid host.
     *
     * @throws GenieException on any problem
     */
    @Test
    public void canGetJobHost() throws GenieException {
        final String jobId = UUID.randomUUID().toString();
        final String hostname = UUID.randomUUID().toString();
        final JobExecution jobExecution = Mockito.mock(JobExecution.class);
        Mockito.when(jobExecution.getHostName()).thenReturn(hostname);
        Mockito.when(this.jobPersistenceService.getJobExecution(Mockito.eq(jobId))).thenReturn(jobExecution);

        Assert.assertThat(this.jobService.getJobHost(jobId), Matchers.is(hostname));
    }
}
