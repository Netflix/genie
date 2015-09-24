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
package com.netflix.genie.core.jpa.services;

import com.netflix.genie.common.dto.Job;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.exceptions.GenieConflictException;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.core.jobmanager.JobManager;
import com.netflix.genie.core.jobmanager.JobManagerFactory;
import com.netflix.genie.core.jpa.entities.JobEntity;
import com.netflix.genie.core.jpa.repositories.JobRepository;
import com.netflix.genie.core.metrics.GenieNodeStatistics;
import com.netflix.genie.core.util.NetUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import java.net.HttpURLConnection;
import java.util.Date;
import java.util.UUID;

/**
 * Tests for the JobServiceJPAImpl class.
 *
 * @author tgianos
 */
public class JobServiceJPAImplTests {

    private JobServiceJPAImpl service;
    private JobRepository jobRepository;
    private GenieNodeStatistics genieNodeStatistics;
    private JobManagerFactory jobManagerFactory;
    private NetUtil netUtil;

    /**
     * Setup the tests.
     */
    @Before
    public void setup() {
        this.jobRepository = Mockito.mock(JobRepository.class);
        this.genieNodeStatistics = Mockito.mock(GenieNodeStatistics.class);
        this.jobManagerFactory = Mockito.mock(JobManagerFactory.class);
        this.netUtil = Mockito.mock(NetUtil.class);
        this.service = new JobServiceJPAImpl(
                this.jobRepository,
                this.genieNodeStatistics,
                this.jobManagerFactory,
                this.netUtil
        );
    }

    /**
     * Test the get job function.
     *
     * @throws GenieException For any problem
     */
    @Test
    @Ignore
    public void testCreateJob() throws GenieException {
    }

    /**
     * Test the get job function.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieConflictException.class)
    public void testCreateJobAlreadyExists() throws GenieException {
        final String id = UUID.randomUUID().toString();
        final Job job = Mockito.mock(Job.class);
        Mockito.when(job.getId()).thenReturn(id);
        Mockito.when(this.jobRepository.exists(id)).thenReturn(true);
        this.service.createJob(job);
    }

    /**
     * Test running the job. Mock interactions as this isn't integration test.
     *
     * @throws GenieException On unexpected issue.
     */
    @Test
    public void testCreateJobThrowsRandomRuntimeException() throws GenieException {
        final String id = UUID.randomUUID().toString();
        final Job job = Mockito.mock(Job.class);
        Mockito.when(job.getId()).thenReturn(id);
        Mockito.when(this.jobRepository.exists(id)).thenReturn(false);
        Mockito.when(this.netUtil.getHostName()).thenReturn(UUID.randomUUID().toString());
        Mockito.when(this.jobRepository.save(Mockito.any(JobEntity.class)))
                .thenThrow(new RuntimeException("junk"));

        try {
            this.service.createJob(job);
            Assert.fail();
        } catch (final GenieException ge) {
            Assert.assertEquals(HttpURLConnection.HTTP_INTERNAL_ERROR, ge.getErrorCode());
        }
    }

    /**
     * Test the get job function.
     *
     * @throws GenieException For any problem
     */
    @Test
    @Ignore
    public void testGetJob() throws GenieException {
    }

    /**
     * Test the get job function.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testGetJobNoJobExists() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jobRepository.findOne(id)).thenReturn(null);
        this.service.getJob(id);
    }

    /**
     * Test touching the job to update the update time.
     *
     * @throws GenieException For any problem
     */
    @Test
    @Ignore
    public void testSetUpdateTime() throws GenieException {
    }

    /**
     * Test touching the job to update the update time.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testSetUpdateTimeNoJob() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jobRepository.findOne(id)).thenReturn(null);
        this.service.setUpdateTime(id);
    }

    /**
     * Test setting the job status.
     *
     * @throws GenieException For any problem
     */
    @Test
    @Ignore
    public void testSetJobStatus() throws GenieException {
    }

    /**
     * Test touching the job to update the update time.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testSetJobStatusNoJob() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jobRepository.findOne(id)).thenReturn(null);
        this.service.setJobStatus(id, JobStatus.SUCCEEDED, null);
    }

    /**
     * Test setting the process id.
     *
     * @throws GenieException For any problem
     */
    @Test
    @Ignore
    public void testSetProcessIdForJob() throws GenieException {
    }

    /**
     * Test setting the process id.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testSetProcessIdForJobNoJob() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jobRepository.findOne(id)).thenReturn(null);
        this.service.setProcessIdForJob(id, 810);
    }

    /**
     * Test setting the command info.
     *
     * @throws GenieException For any problem
     */
    @Test
    @Ignore
    public void testSetCommandInfoForJob() throws GenieException {
    }

    /**
     * Test setting the command info.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testSetCommandInfoForJobNoJob() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jobRepository.findOne(id)).thenReturn(null);
        this.service.setCommandInfoForJob(id, "cmdId", "cmdName");
    }

    /**
     * Test setting the application info.
     *
     * @throws GenieException For any problem
     */
    @Test
    @Ignore
    public void testSetApplicationInfoForJob() throws GenieException {
    }

    /**
     * Test setting the application info.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testSetApplicationInfoForJobNoJob() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jobRepository.findOne(id)).thenReturn(null);
        this.service.setApplicationInfoForJob(id, "appId", "appName");
    }

    /**
     * Test setting the cluster info.
     *
     * @throws GenieException For any problem
     */
    @Test
    @Ignore
    public void testSetClusterInfoForJob() throws GenieException {
    }

    /**
     * Test setting the cluster info.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testSetClusterInfoForJobNoJob() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jobRepository.findOne(id)).thenReturn(null);
        this.service.setClusterInfoForJob(id, "clusterId", "clusterName");
    }

    /**
     * Test getting the job status.
     *
     * @throws GenieException For any problem
     */
    @Test
    @Ignore
    public void testGetJobStatus() throws GenieException {
    }

    /**
     * Test running the job. Mock interactions as this isn't integration test.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testRunJob() throws GenieException {
        final String id = UUID.randomUUID().toString();
        final Job job = Mockito.mock(Job.class);
        final JobEntity jobEntity
                = Mockito.mock(JobEntity.class);
        Mockito.when(job.getId()).thenReturn(id);
        Mockito.when(this.jobRepository.findOne(id)).thenReturn(jobEntity);

        final JobManager manager = Mockito.mock(JobManager.class);
        Mockito.when(this.jobManagerFactory.getJobManager(job)).thenReturn(manager);

        this.service.runJob(job);
        Mockito.verify(this.jobRepository, Mockito.times(1)).findOne(id);
        Mockito.verify(this.jobManagerFactory, Mockito.times(1)).getJobManager(job);
        Mockito.verify(manager, Mockito.times(1)).launch();
        Mockito.verify(jobEntity, Mockito.times(1)).setUpdated(Mockito.any(Date.class));
        Mockito.verify(this.genieNodeStatistics, Mockito.never()).incrGenieFailedJobs();
    }

    /**
     * Test running the job. Mock interactions as this isn't integration test.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieException.class)
    public void testRunJobThrowsException() throws GenieException {
        final String id = UUID.randomUUID().toString();
        final Job job = Mockito.mock(Job.class);
        final JobEntity jobEntity
                = Mockito.mock(JobEntity.class);
        Mockito.when(job.getId()).thenReturn(id);
        Mockito.when(this.jobRepository.findOne(id)).thenReturn(jobEntity);

        final JobManager manager = Mockito.mock(JobManager.class);
        Mockito.when(this.jobManagerFactory.getJobManager(job)).thenThrow(new GenieNotFoundException("Some message"));

        this.service.runJob(job);
        Mockito.verify(this.jobRepository, Mockito.never()).findOne(id);
        Mockito.verify(this.jobManagerFactory, Mockito.times(1)).getJobManager(job);
        Mockito.verify(manager, Mockito.never()).launch();
        Mockito.verify(jobEntity, Mockito.never()).setUpdated(Mockito.any(Date.class));
        Mockito.verify(this.genieNodeStatistics, Mockito.times(1)).incrGenieFailedJobs();
    }
}
