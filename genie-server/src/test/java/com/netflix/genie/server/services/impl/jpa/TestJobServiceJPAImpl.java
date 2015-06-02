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
package com.netflix.genie.server.services.impl.jpa;

import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.netflix.genie.common.exceptions.GenieConflictException;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.model.ClusterCriteria;
import com.netflix.genie.common.model.Job;
import com.netflix.genie.common.model.JobStatus;
import com.netflix.genie.server.jobmanager.JobManager;
import com.netflix.genie.server.jobmanager.JobManagerFactory;
import com.netflix.genie.server.metrics.GenieNodeStatistics;
import com.netflix.genie.server.repository.jpa.JobRepository;
import com.netflix.genie.server.services.JobService;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import javax.inject.Inject;
import javax.validation.ConstraintViolationException;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Tests for the JobServiceJPAImpl class.
 *
 * @author tgianos
 */
@DatabaseSetup("job/init.xml")
public class TestJobServiceJPAImpl extends DBUnitTestBase {

    private static final String JOB_1_ID = "job1";
    private static final String JOB_2_ID = "job2";
    private static final String TAG_1 = "tag1";
    private static final String TAG_2 = "tag2";

    @Inject
    private JobService service;

    /**
     * Test the get job function.
     *
     * @throws GenieException
     */
    @Test
    public void testCreateJob() throws GenieException {
        final String name = UUID.randomUUID().toString();
        final String user = UUID.randomUUID().toString();
        final String version = UUID.randomUUID().toString();
        final String commandArgs = UUID.randomUUID().toString();
        final List<ClusterCriteria> clusterCriterias = new ArrayList<>();
        final ClusterCriteria criteria1 = new ClusterCriteria();
        final Set<String> tags1 = new HashSet<>();
        tags1.add(UUID.randomUUID().toString());
        tags1.add(UUID.randomUUID().toString());
        criteria1.setTags(tags1);
        clusterCriterias.add(criteria1);
        final ClusterCriteria criteria2 = new ClusterCriteria();
        final Set<String> tags2 = new HashSet<>();
        tags2.add(UUID.randomUUID().toString());
        tags2.add(UUID.randomUUID().toString());
        criteria2.setTags(tags2);
        clusterCriterias.add(criteria2);

        final Set<String> commandCriteria = new HashSet<>();
        commandCriteria.add(UUID.randomUUID().toString());
        commandCriteria.add(UUID.randomUUID().toString());

        final Job created = this.service.createJob(
                new Job(
                        user,
                        name,
                        version,
                        commandArgs,
                        commandCriteria,
                        clusterCriterias
                )
        );

        final Job job = this.service.getJob(created.getId());
        Assert.assertNotNull(job.getId());
        Assert.assertEquals(name, job.getName());
        Assert.assertEquals(user, job.getUser());
        Assert.assertEquals(version, job.getVersion());
        Assert.assertEquals(commandArgs, job.getCommandArgs());
        Assert.assertEquals(clusterCriterias.size(), job.getClusterCriterias().size());
        Assert.assertEquals(commandCriteria.size(), job.getCommandCriteria().size());
        Assert.assertEquals(commandCriteria.size(), job.getCommandCriteriaString().split(",").length);
        Assert.assertEquals(JobStatus.INIT, job.getStatus());
        Assert.assertNotNull(job.getHostName());
        Assert.assertNotNull(job.getOutputURI());
        Assert.assertNotNull(job.getKillURI());
    }

    /**
     * Test the get job function.
     *
     * @throws GenieException
     */
    @Test
    public void testCreateJobWithIdAlreadySet() throws GenieException {
        final String id = UUID.randomUUID().toString();
        final String name = UUID.randomUUID().toString();
        final String user = UUID.randomUUID().toString();
        final String version = UUID.randomUUID().toString();
        final String commandArgs = UUID.randomUUID().toString();
        final List<ClusterCriteria> clusterCriterias = new ArrayList<>();
        final ClusterCriteria criteria1 = new ClusterCriteria();
        final Set<String> tags1 = new HashSet<>();
        tags1.add(UUID.randomUUID().toString());
        tags1.add(UUID.randomUUID().toString());
        criteria1.setTags(tags1);
        clusterCriterias.add(criteria1);
        final ClusterCriteria criteria2 = new ClusterCriteria();
        final Set<String> tags2 = new HashSet<>();
        tags2.add(UUID.randomUUID().toString());
        tags2.add(UUID.randomUUID().toString());
        criteria2.setTags(tags2);
        clusterCriterias.add(criteria2);

        final Set<String> commandCriteria = new HashSet<>();
        commandCriteria.add(UUID.randomUUID().toString());
        commandCriteria.add(UUID.randomUUID().toString());

        final Job jobToCreate = new Job(
                user,
                name,
                version,
                commandArgs,
                commandCriteria,
                clusterCriterias
        );
        jobToCreate.setId(id);

        final Job created = this.service.createJob(jobToCreate);

        Assert.assertEquals(id, created.getId());
        final Job job = this.service.getJob(created.getId());
        Assert.assertNotNull(job.getId());
        Assert.assertEquals(name, job.getName());
        Assert.assertEquals(user, job.getUser());
        Assert.assertEquals(version, job.getVersion());
        Assert.assertEquals(commandArgs, job.getCommandArgs());
        Assert.assertEquals(clusterCriterias.size(), job.getClusterCriterias().size());
        Assert.assertEquals(commandCriteria.size(), job.getCommandCriteria().size());
        Assert.assertEquals(commandCriteria.size(), job.getCommandCriteriaString().split(",").length);
        Assert.assertEquals(JobStatus.INIT, job.getStatus());
        Assert.assertNotNull(job.getHostName());
        Assert.assertNotNull(job.getOutputURI());
        Assert.assertNotNull(job.getKillURI());
    }

    /**
     * Test the get job function.
     *
     * @throws GenieException
     */
    @Test(expected = GenieConflictException.class)
    public void testCreateJobAlreadyExists() throws GenieException {
        final String name = UUID.randomUUID().toString();
        final String user = UUID.randomUUID().toString();
        final String version = UUID.randomUUID().toString();
        final String commandArgs = UUID.randomUUID().toString();
        final List<ClusterCriteria> clusterCriterias = new ArrayList<>();
        final ClusterCriteria criteria1 = new ClusterCriteria();
        final Set<String> tags1 = new HashSet<>();
        tags1.add(UUID.randomUUID().toString());
        tags1.add(UUID.randomUUID().toString());
        criteria1.setTags(tags1);
        clusterCriterias.add(criteria1);
        final ClusterCriteria criteria2 = new ClusterCriteria();
        final Set<String> tags2 = new HashSet<>();
        tags2.add(UUID.randomUUID().toString());
        tags2.add(UUID.randomUUID().toString());
        criteria2.setTags(tags2);
        clusterCriterias.add(criteria2);

        final Set<String> commandCriteria = new HashSet<>();
        commandCriteria.add(UUID.randomUUID().toString());
        commandCriteria.add(UUID.randomUUID().toString());

        final Job jobToCreate = new Job(
                user,
                name,
                version,
                commandArgs,
                commandCriteria,
                clusterCriterias
        );
        jobToCreate.setId(JOB_1_ID);
        this.service.createJob(jobToCreate);
    }

    /**
     * Test running the job. Mock interactions as this isn't integration test.
     */
    @Test
    public void testCreateJobThrowsRandomException() {
        final JobRepository jobRepo = Mockito.mock(JobRepository.class);
        final GenieNodeStatistics stats = Mockito.mock(GenieNodeStatistics.class);
        final JobManagerFactory jobManagerFactory = Mockito.mock(JobManagerFactory.class);
        final JobServiceJPAImpl impl = new JobServiceJPAImpl(jobRepo, stats, jobManagerFactory);

        final Job job = Mockito.mock(Job.class);
        Mockito.when(job.getId()).thenReturn(JOB_1_ID);
        Mockito.when(jobRepo.exists(JOB_1_ID)).thenReturn(false);
        Mockito.when(jobRepo.save(job)).thenThrow(new RuntimeException("junk"));

        try {
            impl.createJob(job);
            Assert.fail();
        } catch (final GenieException ge) {
            Assert.assertEquals(
                    HttpURLConnection.HTTP_INTERNAL_ERROR,
                    ge.getErrorCode()
            );
        }
    }

    /**
     * Test the get job function.
     *
     * @throws GenieException
     */
    @Test
    public void testGetJob() throws GenieException {
        final Job job1 = this.service.getJob(JOB_1_ID);
        Assert.assertEquals(JOB_1_ID, job1.getId());
        Assert.assertEquals("testPigJob", job1.getName());
        Assert.assertEquals("tgianos", job1.getUser());
        Assert.assertEquals("2.4", job1.getVersion());
        Assert.assertEquals("-f -j", job1.getCommandArgs());
        Assert.assertEquals(JobStatus.INIT, job1.getStatus());
        Assert.assertEquals(3, job1.getTags().size());
        Assert.assertEquals(2, job1.getCommandCriteria().size());
        Assert.assertEquals(3, job1.getClusterCriterias().size());

        final Job job2 = this.service.getJob(JOB_2_ID);
        Assert.assertEquals(JOB_2_ID, job2.getId());
        Assert.assertEquals("testSparkJob", job2.getName());
        Assert.assertEquals("amsharma", job2.getUser());
        Assert.assertEquals("2.4.3", job2.getVersion());
        Assert.assertEquals("-f -j -a", job2.getCommandArgs());
        Assert.assertEquals(JobStatus.FAILED, job2.getStatus());
        Assert.assertEquals(4, job2.getTags().size());
        Assert.assertEquals(2, job2.getCommandCriteria().size());
        Assert.assertEquals(2, job2.getClusterCriterias().size());
    }

    /**
     * Test the get job function.
     *
     * @throws GenieException
     */
    @Test(expected = ConstraintViolationException.class)
    public void testGetJobNoId() throws GenieException {
        this.service.getJob(null);
    }

    /**
     * Test the get job function.
     *
     * @throws GenieException
     */
    @Test(expected = GenieNotFoundException.class)
    public void testGetJobNoJobExists() throws GenieException {
        this.service.getJob(UUID.randomUUID().toString());
    }

    /**
     * Test the get jobs function.
     */
    @Test
    public void testGetJobsById() {
        final List<Job> jobs = this.service.getJobs(
                JOB_1_ID,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                0,
                10,
                true,
                null
        );
        Assert.assertEquals(1, jobs.size());
        Assert.assertEquals(JOB_1_ID, jobs.get(0).getId());
    }

    /**
     * Test the get jobs function.
     */
    @Test
    public void testGetJobsByName() {
        final List<Job> jobs = this.service.getJobs(
                null,
                "testSparkJob",
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                0,
                10,
                true,
                null
        );
        Assert.assertEquals(1, jobs.size());
        Assert.assertEquals(JOB_2_ID, jobs.get(0).getId());
    }

    /**
     * Test the get jobs function.
     */
    @Test
    public void testGetJobsByUser() {
        final List<Job> jobs = this.service.getJobs(
                null,
                null,
                "tgianos",
                null,
                null,
                null,
                null,
                null,
                null,
                0,
                10,
                true,
                null
        );
        Assert.assertEquals(1, jobs.size());
        Assert.assertEquals(JOB_1_ID, jobs.get(0).getId());
    }

    /**
     * Test the get jobs function.
     */
    @Test
    public void testGetJobsByStatus() {
        final Set<JobStatus> statuses = new HashSet<>();
        statuses.add(JobStatus.FAILED);
        statuses.add(JobStatus.INIT);

        final List<Job> jobs = this.service.getJobs(
                null,
                null,
                null,
                statuses,
                null,
                null,
                null,
                null,
                null,
                0,
                10,
                true,
                null
        );
        Assert.assertEquals(2, jobs.size());
        Assert.assertEquals(JOB_1_ID, jobs.get(0).getId());
        Assert.assertEquals(JOB_2_ID, jobs.get(1).getId());
    }

    /**
     * Test get job by tag.
     *
     * @throws GenieException
     */
    @Test
    public void testGetJobByTag()
            throws GenieException {

        final Set<String> tags = new HashSet<>();
        tags.add(TAG_1);
        tags.add(TAG_2);

        @SuppressWarnings("unchecked")
        final List<Job> jobs = this.service.getJobs(
                null,
                null,
                null,
                null,
                tags,
                null,
                null,
                null,
                null,
                0,
                10,
                true,
                null
        );
        Assert.assertEquals(1, jobs.size());
        Assert.assertEquals(JOB_2_ID, jobs.get(0).getId());
    }

    /**
     * Test the get job by non-existent Tag.
     *
     * @throws GenieException
     */
    @Test
    public void testGetJobWithNonExistentTag()
            throws GenieException {
        final Set<String> tags = new HashSet<>();
        tags.add("random");

        final List<Job> jobs = this.service.getJobs(
                null,
                null,
                null,
                null,
                tags,
                null,
                null,
                null,
                null,
                0,
                10,
                true,
                null
        );

        Assert.assertEquals(0, jobs.size());
    }

    /**
     * Test the get jobs function.
     */
    @Test
    public void testGetJobsByClusterName() {
        final List<Job> jobs = this.service.getJobs(
                null,
                null,
                null,
                null,
                null,
                "h2prod",
                null,
                null,
                null,
                -1,
                0,
                true,
                null
        );
        Assert.assertEquals(1, jobs.size());
        Assert.assertEquals(JOB_1_ID, jobs.get(0).getId());
    }

    /**
     * Test the get jobs function.
     */
    @Test
    public void testGetJobsByCommandName() {
        final List<Job> jobs = this.service.getJobs(
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                "pig_13_prod",
                null,
                -1,
                0,
                true,
                null
        );
        Assert.assertEquals(1, jobs.size());
        Assert.assertEquals(JOB_1_ID, jobs.get(0).getId());
    }

    /**
     * Test the get jobs function.
     */
    @Test
    public void testGetJobsByCommandId() {
        final List<Job> jobs = this.service.getJobs(
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                "command1",
                -1,
                0,
                true,
                null
        );
        Assert.assertEquals(1, jobs.size());
        Assert.assertEquals(JOB_1_ID, jobs.get(0).getId());
    }

    /**
     * Test the get jobs function.
     */
    @Test
    public void testGetJobsByClusterId() {
        final List<Job> jobs = this.service.getJobs(
                null,
                null,
                null,
                null,
                null,
                null,
                "cluster2",
                null,
                null,
                0,
                10,
                true,
                null
        );
        Assert.assertEquals(1, jobs.size());
        Assert.assertEquals(JOB_2_ID, jobs.get(0).getId());
    }

    /**
     * Test the get jobs method with descending sort.
     */
    @Test
    public void testGetClustersDescending() {
        //Default to order by Updated
        final List<Job> jobs = this.service.getJobs(null, null, null, null, null, null, null, null, null,
                0, 10, true, null);
        Assert.assertEquals(2, jobs.size());
        Assert.assertEquals(JOB_1_ID, jobs.get(0).getId());
        Assert.assertEquals(JOB_2_ID, jobs.get(1).getId());
    }

    /**
     * Test the get jobs method with ascending sort.
     */
    @Test
    public void testGetClustersAscending() {
        //Default to order by Updated
        final List<Job> jobs = this.service.getJobs(null, null, null, null, null, null, null, null,
                null, 0, 10, false, null);
        Assert.assertEquals(2, jobs.size());
        Assert.assertEquals(JOB_2_ID, jobs.get(0).getId());
        Assert.assertEquals(JOB_1_ID, jobs.get(1).getId());
    }

    /**
     * Test the get jobs method default order by.
     */
    @Test
    public void testGetClustersOrderBysDefault() {
        //Default to order by Updated
        final List<Job> jobs = this.service.getJobs(null, null, null, null, null, null, null, null, null,
                0, 10, true, null);
        Assert.assertEquals(2, jobs.size());
        Assert.assertEquals(JOB_1_ID, jobs.get(0).getId());
        Assert.assertEquals(JOB_2_ID, jobs.get(1).getId());
    }

    /**
     * Test the get jobs method order by updated.
     */
    @Test
    public void testGetClustersOrderBysUpdated() {
        final Set<String> orderBys = new HashSet<>();
        orderBys.add("updated");
        final List<Job> jobs = this.service.getJobs(null, null, null, null, null, null, null, null,
                null, 0, 10, true, orderBys);
        Assert.assertEquals(2, jobs.size());
        Assert.assertEquals(JOB_1_ID, jobs.get(0).getId());
        Assert.assertEquals(JOB_2_ID, jobs.get(1).getId());
    }

    /**
     * Test the get jobs method order by name.
     */
    @Test
    public void testGetClustersOrderBysName() {
        final Set<String> orderBys = new HashSet<>();
        orderBys.add("name");
        final List<Job> jobs = this.service.getJobs(null, null, null, null, null, null, null, null, null,
                0, 10, true, orderBys);
        Assert.assertEquals(2, jobs.size());
        Assert.assertEquals(JOB_2_ID, jobs.get(0).getId());
        Assert.assertEquals(JOB_1_ID, jobs.get(1).getId());
    }

    /**
     * Test the get jobs method order by an invalid field should return the order by default value (updated).
     */
    @Test
    public void testGetClustersOrderBysInvalidField() {
        final Set<String> orderBys = new HashSet<>();
        orderBys.add("I'mNotAValidField");
        final List<Job> jobs = this.service.getJobs(null, null, null, null, null, null, null,
                null, null, 0, 10, true, orderBys);
        Assert.assertEquals(2, jobs.size());
        Assert.assertEquals(JOB_1_ID, jobs.get(0).getId());
        Assert.assertEquals(JOB_2_ID, jobs.get(1).getId());
    }

    /**
     * Test the get jobs method order by a collection field should return the order by default value (updated).
     */
    @Test
    public void testGetClustersOrderBysCollectionField() {
        final Set<String> orderBys = new HashSet<>();
        orderBys.add("tags");
        final List<Job> jobs = this.service.getJobs(null, null, null, null, null, null, null, null,
                null, 0, 10, true, orderBys);
        Assert.assertEquals(2, jobs.size());
        Assert.assertEquals(JOB_1_ID, jobs.get(0).getId());
        Assert.assertEquals(JOB_2_ID, jobs.get(1).getId());
    }

    /**
     * Test add tags to job.
     *
     * @throws GenieException
     */
    @Test
    public void testAddTagsToJob() throws GenieException {
        final String newTag1 = UUID.randomUUID().toString();
        final String newTag2 = UUID.randomUUID().toString();
        final String newTag3 = UUID.randomUUID().toString();

        final Set<String> newTags = new HashSet<>();
        newTags.add(newTag1);
        newTags.add(newTag2);
        newTags.add(newTag3);

        Assert.assertEquals(3,
                this.service.getTagsForJob(JOB_1_ID).size());
        final Set<String> finalTags
                = this.service.addTagsForJob(JOB_1_ID, newTags);
        Assert.assertEquals(6, finalTags.size());
        Assert.assertTrue(finalTags.contains(newTag1));
        Assert.assertTrue(finalTags.contains(newTag2));
        Assert.assertTrue(finalTags.contains(newTag3));
    }

    /**
     * Test add tags to job.
     *
     * @throws GenieException
     */
    @Test(expected = ConstraintViolationException.class)
    public void testAddTagsToJobNoId() throws GenieException {
        this.service.addTagsForJob(null, new HashSet<String>());
    }

    /**
     * Test add tags to job.
     *
     * @throws GenieException
     */
    @Test(expected = ConstraintViolationException.class)
    public void testAddTagsToJobNoTags() throws GenieException {
        this.service.addTagsForJob(JOB_1_ID, null);
    }

    /**
     * Test add tags to job.
     *
     * @throws GenieException
     */
    @Test(expected = GenieNotFoundException.class)
    public void testAddTagsForJobNoJob() throws GenieException {
        final Set<String> tags = new HashSet<>();
        tags.add(UUID.randomUUID().toString());
        this.service.addTagsForJob(UUID.randomUUID().toString(), tags);
    }

    /**
     * Test update tags for job.
     *
     * @throws GenieException
     */
    @Test
    public void testUpdateTagsForJob() throws GenieException {
        final String newTag1 = UUID.randomUUID().toString();
        final String newTag2 = UUID.randomUUID().toString();
        final String newTag3 = UUID.randomUUID().toString();

        final Set<String> newTags = new HashSet<>();
        newTags.add(newTag1);
        newTags.add(newTag2);
        newTags.add(newTag3);

        Assert.assertEquals(3,
                this.service.getTagsForJob(JOB_1_ID).size());
        final Set<String> finalTags
                = this.service.updateTagsForJob(JOB_1_ID, newTags);
        Assert.assertEquals(3, finalTags.size());
        Assert.assertTrue(finalTags.contains(newTag1));
        Assert.assertTrue(finalTags.contains(newTag2));
        Assert.assertTrue(finalTags.contains(newTag3));
    }

    /**
     * Test update tags for job.
     *
     * @throws GenieException
     */
    @Test(expected = ConstraintViolationException.class)
    public void testUpdateTagsForJobNoId() throws GenieException {
        this.service.updateTagsForJob(null, new HashSet<String>());
    }

    /**
     * Test update tags for job.
     *
     * @throws GenieException
     */
    @Test(expected = GenieNotFoundException.class)
    public void testUpdateTagsForJobNoJob() throws GenieException {
        final Set<String> tags = new HashSet<>();
        tags.add(UUID.randomUUID().toString());
        this.service.updateTagsForJob(UUID.randomUUID().toString(), tags);
    }

    /**
     * Test get tags for job.
     *
     * @throws GenieException
     */
    @Test
    public void testGetTagsForJob() throws GenieException {
        Assert.assertEquals(3,
                this.service.getTagsForJob(JOB_1_ID).size());
    }

    /**
     * Test get tags to job.
     *
     * @throws GenieException
     */
    @Test(expected = ConstraintViolationException.class)
    public void testGetTagsForJobNoId() throws GenieException {
        this.service.getTagsForJob(null);
    }

    /**
     * Test get tags to job.
     *
     * @throws GenieException
     */
    @Test(expected = GenieNotFoundException.class)
    public void testGetTagsForJobNoJob() throws GenieException {
        this.service.getTagsForJob(UUID.randomUUID().toString());
    }

    /**
     * Test remove all tags for job.
     *
     * @throws GenieException
     */
    @Test
    public void testRemoveAllTagsForJob() throws GenieException {
        Assert.assertEquals(3,
                this.service.getTagsForJob(JOB_1_ID).size());
        final Set<String> finalTags
                = this.service.removeAllTagsForJob(JOB_1_ID);
        Assert.assertTrue(finalTags.isEmpty());
    }

    /**
     * Test remove all tags for job.
     *
     * @throws GenieException
     */
    @Test(expected = ConstraintViolationException.class)
    public void testRemoveAllTagsForJobNoId() throws GenieException {
        this.service.removeAllTagsForJob(null);
    }

    /**
     * Test remove all tags for job.
     *
     * @throws GenieException
     */
    @Test(expected = GenieNotFoundException.class)
    public void testRemoveAllTagsForJobNoJob() throws GenieException {
        this.service.removeAllTagsForJob(UUID.randomUUID().toString());
    }

    /**
     * Test remove tag for job.
     *
     * @throws GenieException
     */
    @Test
    public void testRemoveTagForJob() throws GenieException {
        final Set<String> tags
                = this.service.getTagsForJob(JOB_1_ID);
        Assert.assertEquals(3, tags.size());
        Assert.assertEquals(2,
                this.service.removeTagForJob(
                        JOB_1_ID,
                        "2.4").size()
        );
    }

    /**
     * Test remove tag for job.
     *
     * @throws GenieException
     */
    @Test(expected = ConstraintViolationException.class)
    public void testRemoveTagForJobNullTag() throws GenieException {
        this.service.removeTagForJob(JOB_1_ID, null);
    }

    /**
     * Test remove configuration for job.
     *
     * @throws GenieException
     */
    @Test(expected = ConstraintViolationException.class)
    public void testRemoveTagForJobNoId() throws GenieException {
        this.service.removeTagForJob(null, "something");
    }

    /**
     * Test remove configuration for job.
     *
     * @throws GenieException
     */
    @Test(expected = GenieNotFoundException.class)
    public void testRemoveTagForJobNoJob() throws GenieException {
        this.service.removeTagForJob(
                UUID.randomUUID().toString(),
                "something"
        );
    }

    /**
     * Test remove configuration for job.
     *
     * @throws GenieException
     */
    @Test(expected = GeniePreconditionException.class)
    public void testRemoveTagForJobId() throws GenieException {
        this.service.removeTagForJob(
                JOB_1_ID,
                JOB_1_ID
        );
    }

    /**
     * Test touching the job to update the update time.
     *
     * @throws GenieException
     */
    @Test
    public void testSetUpdateTime() throws GenieException {
        final long initialUpdated = this.service.getJob(JOB_1_ID).getUpdated().getTime();
        final long newUpdated = this.service.setUpdateTime(JOB_1_ID);
        Assert.assertNotEquals(initialUpdated, newUpdated);
    }

    /**
     * Test touching the job to update the update time.
     *
     * @throws GenieException
     */
    @Test(expected = ConstraintViolationException.class)
    public void testSetUpdateTimeNoId() throws GenieException {
        this.service.setUpdateTime(null);
    }

    /**
     * Test touching the job to update the update time.
     *
     * @throws GenieException
     */
    @Test(expected = GenieNotFoundException.class)
    public void testSetUpdateTimeNoJob() throws GenieException {
        this.service.setUpdateTime(UUID.randomUUID().toString());
    }

    /**
     * Test setting the job status.
     *
     * @throws GenieException
     */
    @Test
    public void testSetJobStatus() throws GenieException {
        final String msg = UUID.randomUUID().toString();
        this.service.setJobStatus(JOB_1_ID, JobStatus.RUNNING, msg);
        final Job job = this.service.getJob(JOB_1_ID);
        Assert.assertEquals(JobStatus.RUNNING, job.getStatus());
        Assert.assertEquals(msg, job.getStatusMsg());
    }

    /**
     * Test touching the job to update the update time.
     *
     * @throws GenieException
     */
    @Test(expected = ConstraintViolationException.class)
    public void testSetJobStatusNoId() throws GenieException {
        this.service.setJobStatus(null, null, null);
    }

    /**
     * Test touching the job to update the update time.
     *
     * @throws GenieException
     */
    @Test(expected = ConstraintViolationException.class)
    public void testSetJobStatusNoStatus() throws GenieException {
        this.service.setJobStatus(JOB_1_ID, null, null);
    }

    /**
     * Test touching the job to update the update time.
     *
     * @throws GenieException
     */
    @Test
    public void testSetJobStatusNoMessage() throws GenieException {
        this.service.setJobStatus(JOB_1_ID, JobStatus.SUCCEEDED, null);
        final Job job = this.service.getJob(JOB_1_ID);
        Assert.assertEquals(JobStatus.SUCCEEDED, job.getStatus());
        Assert.assertNull(job.getStatusMsg());
    }

    /**
     * Test touching the job to update the update time.
     *
     * @throws GenieException
     */
    @Test(expected = GenieNotFoundException.class)
    public void testSetJobStatusNoJob() throws GenieException {
        this.service.setJobStatus(
                UUID.randomUUID().toString(),
                JobStatus.SUCCEEDED,
                null
        );
    }

    /**
     * Test setting the process id.
     *
     * @throws GenieException
     */
    @Test
    public void testSetProcessIdForJob() throws GenieException {
        Assert.assertEquals(-1, this.service.getJob(JOB_1_ID).getProcessHandle());
        final Random random = new Random();
        int pid = -1;
        while (pid < 0) {
            pid = random.nextInt();
        }
        this.service.setProcessIdForJob(JOB_1_ID, pid);
        Assert.assertEquals(pid, this.service.getJob(JOB_1_ID).getProcessHandle());
    }

    /**
     * Test setting the process id.
     *
     * @throws GenieException
     */
    @Test(expected = ConstraintViolationException.class)
    public void testSetProcessIdForJobNoId() throws GenieException {
        this.service.setProcessIdForJob(null, 810);
    }

    /**
     * Test setting the process id.
     *
     * @throws GenieException
     */
    @Test(expected = GenieNotFoundException.class)
    public void testSetProcessIdForJobNoJob() throws GenieException {
        this.service.setProcessIdForJob(UUID.randomUUID().toString(), 810);
    }

    /**
     * Test setting the command info.
     *
     * @throws GenieException
     */
    @Test
    public void testSetCommandInfoForJob() throws GenieException {
        final String id = UUID.randomUUID().toString();
        final String name = UUID.randomUUID().toString();
        this.service.setCommandInfoForJob(JOB_1_ID, id, name);
        final Job job = this.service.getJob(JOB_1_ID);
        Assert.assertEquals(id, job.getCommandId());
        Assert.assertEquals(name, job.getCommandName());
    }

    /**
     * Test setting the command info.
     *
     * @throws GenieException
     */
    @Test(expected = ConstraintViolationException.class)
    public void testSetCommandInfoForJobNoId() throws GenieException {
        this.service.setCommandInfoForJob(null, null, null);
    }

    /**
     * Test setting the command info.
     *
     * @throws GenieException
     */
    @Test(expected = GenieNotFoundException.class)
    public void testSetCommandInfoForJobNoJob() throws GenieException {
        this.service.setCommandInfoForJob(UUID.randomUUID().toString(), "cmdId", "cmdName");
    }

    /**
     * Test setting the application info.
     *
     * @throws GenieException
     */
    @Test
    public void testSetApplicationInfoForJob() throws GenieException {
        final String id = UUID.randomUUID().toString();
        final String name = UUID.randomUUID().toString();
        this.service.setApplicationInfoForJob(JOB_1_ID, id, name);
        final Job job = this.service.getJob(JOB_1_ID);
        Assert.assertEquals(id, job.getApplicationId());
        Assert.assertEquals(name, job.getApplicationName());
    }

    /**
     * Test setting the application info.
     *
     * @throws GenieException
     */
    @Test(expected = ConstraintViolationException.class)
    public void testSetApplicationInfoForJobNoId() throws GenieException {
        this.service.setApplicationInfoForJob(null, null, null);
    }

    /**
     * Test setting the application info.
     *
     * @throws GenieException
     */
    @Test(expected = GenieNotFoundException.class)
    public void testSetApplicationInfoForJobNoJob() throws GenieException {
        this.service.setApplicationInfoForJob(UUID.randomUUID().toString(), "appId", "appName");
    }

    /**
     * Test setting the cluster info.
     *
     * @throws GenieException
     */
    @Test
    public void testSetClusterInfoForJob() throws GenieException {
        final String id = UUID.randomUUID().toString();
        final String name = UUID.randomUUID().toString();
        this.service.setClusterInfoForJob(JOB_1_ID, id, name);
        final Job job = this.service.getJob(JOB_1_ID);
        Assert.assertEquals(id, job.getExecutionClusterId());
        Assert.assertEquals(name, job.getExecutionClusterName());
    }

    /**
     * Test setting the cluster info.
     *
     * @throws GenieException
     */
    @Test(expected = ConstraintViolationException.class)
    public void testSetClusterInfoForJobNoId() throws GenieException {
        this.service.setClusterInfoForJob(null, null, null);
    }

    /**
     * Test setting the cluster info.
     *
     * @throws GenieException
     */
    @Test(expected = GenieNotFoundException.class)
    public void testSetClusterInfoForJobNoJob() throws GenieException {
        this.service.setClusterInfoForJob(UUID.randomUUID().toString(), "clusterId", "clusterName");
    }

    /**
     * Test getting the job status.
     *
     * @throws GenieException
     */
    @Test
    public void testGetJobStatus() throws GenieException {
        Assert.assertEquals(JobStatus.FAILED, this.service.getJobStatus(JOB_2_ID));
    }

    /**
     * Test running the job. Mock interactions as this isn't integration test.
     *
     * @throws GenieException
     */
    @Test
    public void testRunJob() throws GenieException {
        final JobRepository jobRepo = Mockito.mock(JobRepository.class);
        final GenieNodeStatistics stats = Mockito.mock(GenieNodeStatistics.class);
        final JobManagerFactory jobManagerFactory = Mockito.mock(JobManagerFactory.class);
        final JobServiceJPAImpl impl = new JobServiceJPAImpl(jobRepo, stats, jobManagerFactory);

        final Job job = Mockito.mock(Job.class);
        Mockito.when(job.getId()).thenReturn(JOB_1_ID);
        Mockito.when(jobRepo.findOne(JOB_1_ID)).thenReturn(job);

        final JobManager manager = Mockito.mock(JobManager.class);
        Mockito.when(jobManagerFactory.getJobManager(job)).thenReturn(manager);

        impl.runJob(job);
        Mockito.verify(jobRepo, Mockito.times(1)).findOne(JOB_1_ID);
        Mockito.verify(jobManagerFactory, Mockito.times(1)).getJobManager(job);
        Mockito.verify(manager, Mockito.times(1)).launch();
        Mockito.verify(job, Mockito.times(1)).setUpdated(Mockito.any(Date.class));
        Mockito.verify(stats, Mockito.never()).incrGenieFailedJobs();
    }

    /**
     * Test running the job. Mock interactions as this isn't integration test.
     *
     * @throws GenieException
     */
    @Test(expected = GenieException.class)
    public void testRunJobThrowsException() throws GenieException {
        final JobRepository jobRepo = Mockito.mock(JobRepository.class);
        final GenieNodeStatistics stats = Mockito.mock(GenieNodeStatistics.class);
        final JobManagerFactory jobManagerFactory = Mockito.mock(JobManagerFactory.class);
        final JobServiceJPAImpl impl = new JobServiceJPAImpl(jobRepo, stats, jobManagerFactory);

        final Job job = Mockito.mock(Job.class);
        Mockito.when(job.getId()).thenReturn(JOB_1_ID);
        Mockito.when(jobRepo.findOne(JOB_1_ID)).thenReturn(job);

        final JobManager manager = Mockito.mock(JobManager.class);
        Mockito.when(jobManagerFactory.getJobManager(job))
                .thenThrow(new GenieException(
                                HttpURLConnection.HTTP_NOT_FOUND,
                                "Some message"
                        ));

        impl.runJob(job);
        Mockito.verify(jobRepo, Mockito.never()).findOne(JOB_1_ID);
        Mockito.verify(jobManagerFactory, Mockito.times(1)).getJobManager(job);
        Mockito.verify(manager, Mockito.never()).launch();
        Mockito.verify(job, Mockito.never()).setUpdated(Mockito.any(Date.class));
        Mockito.verify(stats, Mockito.times(1)).incrGenieFailedJobs();
    }
}
