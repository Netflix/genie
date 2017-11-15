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

import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.github.springtestdbunit.annotation.DatabaseTearDown;
import com.netflix.genie.core.jobs.JobConstants;
import com.netflix.genie.core.jpa.repositories.JpaJobRepository;
import com.netflix.genie.core.services.JobPersistenceService;
import com.netflix.genie.test.categories.IntegrationTest;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Calendar;

/**
 * Integration tests for JpaJobPersistenceImpl.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(IntegrationTest.class)
@DatabaseSetup("JpaJobPersistenceServiceImplIntegrationTests/init.xml")
@DatabaseTearDown("cleanup.xml")
public class JpaJobPersistenceImplIntegrationTests extends DBUnitTestBase {

    private static final String JOB_3_ID = "job3";

    @Autowired
    private JpaJobRepository jobRepository;
    @Autowired
    private JobPersistenceService jobPersistenceService;

    /**
     * Setup.
     */
    @Before
    public void setup() {
        Assert.assertThat(this.jobRepository.count(), Matchers.is(3L));
    }

    /**
     * Make sure we can delete jobs that were created before a given date.
     */
    @Test
    public void canDeleteJobsCreatedBeforeDateWithMinTransactionAndPageSize() {
        // Try to delete a single job from before Jan 1, 2016
        final Calendar cal = Calendar.getInstance(JobConstants.UTC);
        cal.set(2016, Calendar.JANUARY, 1, 0, 0, 0);
        cal.set(Calendar.MILLISECOND, 0);

        final long deleted = this.jobPersistenceService.deleteBatchOfJobsCreatedBeforeDate(cal.getTime(), 1, 1);

        Assert.assertThat(deleted, Matchers.is(1L));
        Assert.assertThat(this.jobRepository.count(), Matchers.is(2L));
        Assert.assertTrue(this.jobRepository.findByUniqueId(JOB_3_ID).isPresent());
    }

    /**
     * Make sure we can delete jobs that were created before a given date.
     */
    @Test
    public void canDeleteJobsCreatedBeforeDateWithPageLargerThanMax() {
        // Try to delete a all jobs from before Jan 1, 2016
        final Calendar cal = Calendar.getInstance(JobConstants.UTC);
        cal.set(2016, Calendar.JANUARY, 1, 0, 0, 0);
        cal.set(Calendar.MILLISECOND, 0);

        final long deleted = this.jobPersistenceService.deleteBatchOfJobsCreatedBeforeDate(cal.getTime(), 1, 10);

        Assert.assertThat(deleted, Matchers.is(2L));
        Assert.assertThat(this.jobRepository.count(), Matchers.is(1L));
        Assert.assertTrue(this.jobRepository.findByUniqueId(JOB_3_ID).isPresent());
    }

    /**
     * Make sure we can delete jobs that were created before a given date.
     */
    @Test
    public void canDeleteJobsCreatedBeforeDateWithMaxLargerThanPage() {
        // Try to delete a all jobs from before Jan 1, 2016
        final Calendar cal = Calendar.getInstance(JobConstants.UTC);
        cal.set(2016, Calendar.JANUARY, 1, 0, 0, 0);
        cal.set(Calendar.MILLISECOND, 0);

        final long deleted = this.jobPersistenceService.deleteBatchOfJobsCreatedBeforeDate(cal.getTime(), 10, 1);

        Assert.assertThat(deleted, Matchers.is(2L));
        Assert.assertThat(this.jobRepository.count(), Matchers.is(1L));
        Assert.assertTrue(this.jobRepository.findByUniqueId(JOB_3_ID).isPresent());
    }

    /**
     * Make sure we can delete jobs that were created before a given date.
     */
    @Test
    public void canDeleteJobsCreatedBeforeDateWithLargeTransactionAndPageSize() {
        // Try to delete all jobs before Jan 1, 2016
        final Calendar cal = Calendar.getInstance(JobConstants.UTC);
        cal.set(2016, Calendar.JANUARY, 1, 0, 0, 0);
        cal.set(Calendar.MILLISECOND, 0);

        final long deleted = this.jobPersistenceService.deleteBatchOfJobsCreatedBeforeDate(cal.getTime(), 10_000, 1);

        Assert.assertThat(deleted, Matchers.is(2L));
        Assert.assertThat(this.jobRepository.count(), Matchers.is(1L));
        Assert.assertTrue(this.jobRepository.findByUniqueId(JOB_3_ID).isPresent());
    }
}
