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

import org.junit.Ignore;

/**
 * Tests for the JobServiceJPAImpl class.
 *
 * @author tgianos
 */
@Ignore
//@DatabaseSetup("JobServiceJPAImplIntegrationTests/init.xml")
//@DatabaseTearDown("cleanup.xml")
public class JpaJobServiceImplIntegrationTests extends DBUnitTestBase {

//    private static final String JOB_1_ID = "job1";
//    private static final String JOB_2_ID = "job2";
//    private static final String TAG_1 = "tag1";
//    private static final String TAG_2 = "tag2";
//
//    private static final Pageable PAGE = new PageRequest(0, 10, Sort.Direction.DESC, "updated");
//
//    @Autowired
//    private OldJobService service;
//
//    @Autowired
//    private JpaJobRepository jpaJobRepository;
//
//    /**
//     * Test the get job function.
//     *
//     * @throws GenieException For any problem
//     */
//    @Test
//    @Ignore
//    public void testCreateJob() throws GenieException {
//        final String name = UUID.randomUUID().toString();
//        final String user = UUID.randomUUID().toString();
//        final String version = UUID.randomUUID().toString();
//        final String commandArgs = UUID.randomUUID().toString();
//        final List<ClusterCriteria> clusterCriterias = new ArrayList<>();
//        final ClusterCriteria criteria1
//                = new ClusterCriteria(Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString()));
//        clusterCriterias.add(criteria1);
//        final ClusterCriteria criteria2
//                = new ClusterCriteria(Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString()));
//        clusterCriterias.add(criteria2);
//
//        final Set<String> commandCriteria = new HashSet<>();
//        commandCriteria.add(UUID.randomUUID().toString());
//        commandCriteria.add(UUID.randomUUID().toString());
//
//        final Job created = this.service.createJob(
//                new Job.Builder(
//                        user,
//                        name,
//                        version,
//                        commandArgs,
//                        clusterCriterias,
//                        commandCriteria
//                ).build()
//        );
//
//        final Job job = this.service.getJob(created.getId());
//        Assert.assertNotNull(job.getId());
//        Assert.assertEquals(name, job.getName());
//        Assert.assertEquals(user, job.getUser());
//        Assert.assertEquals(version, job.getVersion());
//        Assert.assertEquals(commandArgs, job.getCommandArgs());
//        Assert.assertEquals(clusterCriterias.size(), job.getClusterCriterias().size());
//        Assert.assertEquals(commandCriteria.size(), job.getCommandCriteria().size());
//        Assert.assertEquals(JobStatus.INIT, job.getStatus());
//        Assert.assertNotNull(job.getHostName());
//        Assert.assertNotNull(job.getOutputURI());
//        Assert.assertNotNull(job.getKillURI());
//    }
//
//    /**
//     * Test the get job function.
//     *
//     * @throws GenieException For any problem
//     */
//    @Test
//    @Ignore
//    public void testCreateJobWithIdAlreadySet() throws GenieException {
//        final String id = UUID.randomUUID().toString();
//        final String name = UUID.randomUUID().toString();
//        final String user = UUID.randomUUID().toString();
//        final String version = UUID.randomUUID().toString();
//        final String commandArgs = UUID.randomUUID().toString();
//        final List<ClusterCriteria> clusterCriterias = new ArrayList<>();
//        final ClusterCriteria criteria1
//                = new ClusterCriteria(Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString()));
//        clusterCriterias.add(criteria1);
//        final ClusterCriteria criteria2
//                = new ClusterCriteria(Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString()));
//        clusterCriterias.add(criteria2);
//
//        final Set<String> commandCriteria = new HashSet<>();
//        commandCriteria.add(UUID.randomUUID().toString());
//        commandCriteria.add(UUID.randomUUID().toString());
//
//        final Job jobToCreate = new Job.Builder(
//                user,
//                name,
//                version,
//                commandArgs,
//                clusterCriterias,
//                commandCriteria
//        ).withId(id).build();
//
//        final Job created = this.service.createJob(jobToCreate);
//
//        Assert.assertEquals(id, created.getId());
//        final Job job = this.service.getJob(created.getId());
//        Assert.assertNotNull(job.getId());
//        Assert.assertEquals(name, job.getName());
//        Assert.assertEquals(user, job.getUser());
//        Assert.assertEquals(version, job.getVersion());
//        Assert.assertEquals(commandArgs, job.getCommandArgs());
//        Assert.assertEquals(clusterCriterias.size(), job.getClusterCriterias().size());
//        Assert.assertEquals(commandCriteria.size(), job.getCommandCriteria().size());
//        Assert.assertEquals(JobStatus.INIT, job.getStatus());
//        Assert.assertNotNull(job.getHostName());
//        Assert.assertNotNull(job.getOutputURI());
//        Assert.assertNotNull(job.getKillURI());
//    }
//
//    /**
//     * Test the get job function.
//     *
//     * @throws GenieException For any problem
//     */
//    @Test
//    public void testGetJob() throws GenieException {
//        final Job job1 = this.service.getJob(JOB_1_ID);
//        Assert.assertEquals(JOB_1_ID, job1.getId());
//        Assert.assertEquals("testPigJob", job1.getName());
//        Assert.assertEquals("tgianos", job1.getUser());
//        Assert.assertEquals("2.4", job1.getVersion());
//        Assert.assertEquals("-f -j", job1.getCommandArgs());
//        Assert.assertEquals(JobStatus.INIT, job1.getStatus());
//        Assert.assertNotNull(job1.getTags());
//        Assert.assertEquals(3, job1.getTags().size());
//        Assert.assertEquals(2, job1.getCommandCriteria().size());
//        Assert.assertEquals(3, job1.getClusterCriterias().size());
//
//        final Job job2 = this.service.getJob(JOB_2_ID);
//        Assert.assertEquals(JOB_2_ID, job2.getId());
//        Assert.assertEquals("testSparkJob", job2.getName());
//        Assert.assertEquals("amsharma", job2.getUser());
//        Assert.assertEquals("2.4.3", job2.getVersion());
//        Assert.assertEquals("-f -j -a", job2.getCommandArgs());
//        Assert.assertEquals(JobStatus.FAILED, job2.getStatus());
//        Assert.assertNotNull(job2.getTags());
//        Assert.assertEquals(4, job2.getTags().size());
//        Assert.assertEquals(2, job2.getCommandCriteria().size());
//        Assert.assertEquals(2, job2.getClusterCriterias().size());
//    }
//
//    /**
//     * Test the get job function.
//     *
//     * @throws GenieException For any problem
//     */
//    @Test(expected = ConstraintViolationException.class)
//    public void testGetJobNoId() throws GenieException {
//        this.service.getJob(null);
//    }
//
//    /**
//     * Test the get jobs function.
//     */
//    @Test
//    public void testGetJobsById() {
//        final Page<Job> jobs = this.service.getJobs(
//                JOB_1_ID,
//                null,
//                null,
//                null,
//                null,
//                null,
//                null,
//                null,
//                null,
//                PAGE
//        );
//        Assert.assertEquals(1, jobs.getNumberOfElements());
//        Assert.assertEquals(JOB_1_ID, jobs.getContent().get(0).getId());
//    }
//
//    /**
//     * Test the get jobs function.
//     */
//    @Test
//    public void testGetJobsByName() {
//        final Page<Job> jobs = this.service.getJobs(
//                null,
//                "testSparkJob",
//                null,
//                null,
//                null,
//                null,
//                null,
//                null,
//                null,
//                PAGE
//        );
//        Assert.assertEquals(1, jobs.getNumberOfElements());
//        Assert.assertEquals(JOB_2_ID, jobs.getContent().get(0).getId());
//    }
//
//    /**
//     * Test the get jobs function.
//     */
//    @Test
//    public void testGetJobsByUser() {
//        final Page<Job> jobs = this.service.getJobs(
//                null,
//                null,
//                "tgianos",
//                null,
//                null,
//                null,
//                null,
//                null,
//                null,
//                PAGE
//        );
//        Assert.assertEquals(1, jobs.getNumberOfElements());
//        Assert.assertEquals(JOB_1_ID, jobs.getContent().get(0).getId());
//    }
//
//    /**
//     * Test the get jobs function.
//     */
//    @Test
//    public void testGetJobsByStatus() {
//        final Set<JobStatus> statuses = new HashSet<>();
//        statuses.add(JobStatus.FAILED);
//        statuses.add(JobStatus.INIT);
//
//        final Page<Job> jobs = this.service.getJobs(
//                null,
//                null,
//                null,
//                statuses,
//                null,
//                null,
//                null,
//                null,
//                null,
//                PAGE
//        );
//        Assert.assertEquals(2, jobs.getNumberOfElements());
//        Assert.assertEquals(JOB_1_ID, jobs.getContent().get(0).getId());
//        Assert.assertEquals(JOB_2_ID, jobs.getContent().get(1).getId());
//    }
//
//    /**
//     * Test get job by tag.
//     *
//     * @throws GenieException For any problem
//     */
//    @Test
//    public void testGetJobByTag() throws GenieException {
//        final Set<String> tags = new HashSet<>();
//        tags.add(TAG_1);
//        tags.add(TAG_2);
//
//        final Page<Job> jobs = this.service.getJobs(
//                null,
//                null,
//                null,
//                null,
//                tags,
//                null,
//                null,
//                null,
//                null,
//                PAGE
//        );
//        Assert.assertEquals(1, jobs.getNumberOfElements());
//        Assert.assertEquals(JOB_2_ID, jobs.getContent().get(0).getId());
//    }
//
//    /**
//     * Test the get job by non-existent Tag.
//     *
//     * @throws GenieException For any problem
//     */
//    @Test
//    public void testGetJobWithNonExistentTag() throws GenieException {
//        final Set<String> tags = new HashSet<>();
//        tags.add("random");
//
//        final Page<Job> jobs = this.service.getJobs(
//                null,
//                null,
//                null,
//                null,
//                tags,
//                null,
//                null,
//                null,
//                null,
//                PAGE
//        );
//
//        Assert.assertEquals(0, jobs.getNumberOfElements());
//    }
//
//    /**
//     * Test the get jobs function.
//     */
//    @Test
//    public void testGetJobsByClusterName() {
//        final Page<Job> jobs = this.service.getJobs(
//                null,
//                null,
//                null,
//                null,
//                null,
//                "h2prod",
//                null,
//                null,
//                null,
//                PAGE
//        );
//        Assert.assertEquals(1, jobs.getNumberOfElements());
//        Assert.assertEquals(JOB_1_ID, jobs.getContent().get(0).getId());
//    }
//
//    /**
//     * Test the get jobs function.
//     */
//    @Test
//    public void testGetJobsByCommandName() {
//        final Page<Job> jobs = this.service.getJobs(
//                null,
//                null,
//                null,
//                null,
//                null,
//                null,
//                null,
//                "pig_13_prod",
//                null,
//                PAGE
//        );
//        Assert.assertEquals(1, jobs.getNumberOfElements());
//        Assert.assertEquals(JOB_1_ID, jobs.getContent().get(0).getId());
//    }
//
//    /**
//     * Test the get jobs function.
//     */
//    @Test
//    public void testGetJobsByCommandId() {
//        final Page<Job> jobs = this.service.getJobs(
//                null,
//                null,
//                null,
//                null,
//                null,
//                null,
//                null,
//                null,
//                "command1",
//                PAGE
//        );
//        Assert.assertEquals(1, jobs.getNumberOfElements());
//        Assert.assertEquals(JOB_1_ID, jobs.getContent().get(0).getId());
//    }
//
//    /**
//     * Test the get jobs function.
//     */
//    @Test
//    public void testGetJobsByClusterId() {
//        final Page<Job> jobs = this.service.getJobs(
//                null,
//                null,
//                null,
//                null,
//                null,
//                null,
//                "cluster2",
//                null,
//                null,
//                PAGE
//        );
//        Assert.assertEquals(1, jobs.getNumberOfElements());
//        Assert.assertEquals(JOB_2_ID, jobs.getContent().get(0).getId());
//    }
//
//    /**
//     * Test the get jobs method with ascending sort.
//     */
//    @Test
//    public void testGetJobsAscending() {
//        final Pageable ascending = new PageRequest(0, 10, Sort.Direction.ASC, "updated");
//        //Default to order by Updated
//        final Page<Job> jobs = this.service.getJobs(null, null, null, null, null, null, null, null,
//                null, ascending);
//        Assert.assertEquals(2, jobs.getNumberOfElements());
//        Assert.assertEquals(JOB_2_ID, jobs.getContent().get(0).getId());
//        Assert.assertEquals(JOB_1_ID, jobs.getContent().get(1).getId());
//    }
//
//    /**
//     * Test the get jobs method order by name.
//     */
//    @Test
//    public void testGetJobsOrderBysName() {
//        final Pageable name = new PageRequest(0, 10, Sort.Direction.DESC, "name");
//        final Page<Job> jobs = this.service.getJobs(null, null, null, null, null, null, null, null, null, name);
//        Assert.assertEquals(2, jobs.getNumberOfElements());
//        Assert.assertEquals(JOB_2_ID, jobs.getContent().get(0).getId());
//        Assert.assertEquals(JOB_1_ID, jobs.getContent().get(1).getId());
//    }
//
//    /**
//     * Test the get jobs method order by an invalid field should return the order by default value (updated).
//     */
//    @Test(expected = PropertyReferenceException.class)
//    public void testGetJobsOrderBysInvalidField() {
//        final Pageable invalid = new PageRequest(0, 10, Sort.Direction.DESC, "I'mNotAValidField");
//        final Page<Job> jobs = this.service.getJobs(null, null, null, null, null, null, null, null, null, invalid);
//        Assert.assertEquals(2, jobs.getNumberOfElements());
//        Assert.assertEquals(JOB_1_ID, jobs.getContent().get(0).getId());
//        Assert.assertEquals(JOB_2_ID, jobs.getContent().get(1).getId());
//    }
//
//    /**
//     * Test the get jobs method order by a collection field should return the order by default value (updated).
//     */
//    @Ignore
//    @Test
//    public void testGetJobsOrderBysCollectionField() {
//        final Pageable tags = new PageRequest(0, 10, Sort.Direction.DESC, "tags");
//        final Page<Job> jobs = this.service.getJobs(null, null, null, null, null, null, null, null, null, tags);
//        Assert.assertEquals(2, jobs.getNumberOfElements());
//        Assert.assertEquals(JOB_1_ID, jobs.getContent().get(0).getId());
//        Assert.assertEquals(JOB_2_ID, jobs.getContent().get(1).getId());
//    }
//
//    /**
//     * Test touching the job to update the update time.
//     *
//     * @throws GenieException For any problem
//     */
//    @Test
//    public void testSetUpdateTime() throws GenieException {
//        final long initialUpdated = this.service.getJob(JOB_1_ID).getUpdated().getTime();
//        final long newUpdated = this.service.setUpdateTime(JOB_1_ID);
//        Assert.assertNotEquals(initialUpdated, newUpdated);
//    }
//
//    /**
//     * Test touching the job to update the update time.
//     *
//     * @throws GenieException For any problem
//     */
//    @Test(expected = ConstraintViolationException.class)
//    public void testSetUpdateTimeNoId() throws GenieException {
//        this.service.setUpdateTime(null);
//    }
//
//    /**
//     * Test setting the job status.
//     *
//     * @throws GenieException For any problem
//     */
//    @Test
//    public void testSetJobStatus() throws GenieException {
//        final String msg = UUID.randomUUID().toString();
//        this.service.setJobStatus(JOB_1_ID, JobStatus.RUNNING, msg);
//        final Job job = this.service.getJob(JOB_1_ID);
//        Assert.assertEquals(JobStatus.RUNNING, job.getStatus());
//        Assert.assertEquals(msg, job.getStatusMsg());
//    }
//
//    /**
//     * Test touching the job to update the update time.
//     *
//     * @throws GenieException For any problem
//     */
//    @Test(expected = ConstraintViolationException.class)
//    public void testSetJobStatusNoId() throws GenieException {
//        this.service.setJobStatus(null, null, null);
//    }
//
//    /**
//     * Test touching the job to update the update time.
//     *
//     * @throws GenieException For any problem
//     */
//    @Test(expected = ConstraintViolationException.class)
//    public void testSetJobStatusNoStatus() throws GenieException {
//        this.service.setJobStatus(JOB_1_ID, null, null);
//    }
//
//    /**
//     * Test touching the job to update the update time.
//     *
//     * @throws GenieException For any problem
//     */
//    @Test
//    public void testSetJobStatusNoMessage() throws GenieException {
//        this.service.setJobStatus(JOB_1_ID, JobStatus.SUCCEEDED, null);
//        final Job job = this.service.getJob(JOB_1_ID);
//        Assert.assertEquals(JobStatus.SUCCEEDED, job.getStatus());
//        Assert.assertNull(job.getStatusMsg());
//    }
//
//    /**
//     * Test setting the process id.
//     *
//     * @throws GenieException For any problem
//     */
//    @Test
//    public void testSetProcessIdForJob() throws GenieException {
//        Assert.assertEquals(-1, this.service.getJob(JOB_1_ID).getProcessId());
//        final Random random = new Random();
//        int pid = -1;
//        while (pid < 0) {
//            pid = random.nextInt();
//        }
//        this.service.setProcessIdForJob(JOB_1_ID, pid);
//        Assert.assertEquals(pid, this.service.getJob(JOB_1_ID).getProcessId());
//    }
//
//    /**
//     * Test setting the process id.
//     *
//     * @throws GenieException For any problem
//     */
//    @Test(expected = ConstraintViolationException.class)
//    public void testSetProcessIdForJobNoId() throws GenieException {
//        this.service.setProcessIdForJob(null, 810);
//    }
//
//    /**
//     * Test setting the command info.
//     *
//     * @throws GenieException For any problem
//     */
//    @Test
//    public void testSetCommandInfoForJob() throws GenieException {
//        final String id = UUID.randomUUID().toString();
//        final String name = UUID.randomUUID().toString();
//        this.service.setCommandInfoForJob(JOB_1_ID, id, name);
//        final Job job = this.service.getJob(JOB_1_ID);
//        Assert.assertEquals(id, job.getCommandId());
//        Assert.assertEquals(name, this.jpaJobRepository.findOne(JOB_1_ID).getCommandName());
//    }
//
//    /**
//     * Test setting the command info.
//     *
//     * @throws GenieException For any problem
//     */
//    @Test(expected = ConstraintViolationException.class)
//    public void testSetCommandInfoForJobNoId() throws GenieException {
//        this.service.setCommandInfoForJob(null, null, null);
//    }
//
//    /**
//     * Test setting the application info.
//     *
//     * @throws GenieException For any problem
//     */
//    @Test
//    public void testSetApplicationInfoForJob() throws GenieException {
//        final String id = UUID.randomUUID().toString();
//        final String name = UUID.randomUUID().toString();
//        this.service.setApplicationInfoForJob(JOB_1_ID, id, name);
//        Assert.assertEquals(id, this.jpaJobRepository.findOne(JOB_1_ID).getApplicationId());
//        Assert.assertEquals(name, this.jpaJobRepository.findOne(JOB_1_ID).getApplicationName());
//    }
//
//    /**
//     * Test setting the application info.
//     *
//     * @throws GenieException For any problem
//     */
//    @Test(expected = ConstraintViolationException.class)
//    public void testSetApplicationInfoForJobNoId() throws GenieException {
//        this.service.setApplicationInfoForJob(null, null, null);
//    }
//
//    /**
//     * Test setting the cluster info.
//     *
//     * @throws GenieException For any problem
//     */
//    @Test
//    public void testSetClusterInfoForJob() throws GenieException {
//        final String id = UUID.randomUUID().toString();
//        final String name = UUID.randomUUID().toString();
//        this.service.setClusterInfoForJob(JOB_1_ID, id, name);
//        final Job job = this.service.getJob(JOB_1_ID);
//        Assert.assertEquals(id, job.getClusterId());
//        Assert.assertEquals(name, this.jpaJobRepository.findOne(JOB_1_ID).getExecutionClusterName());
//    }
//
//    /**
//     * Test setting the cluster info.
//     *
//     * @throws GenieException For any problem
//     */
//    @Test(expected = ConstraintViolationException.class)
//    public void testSetClusterInfoForJobNoId() throws GenieException {
//        this.service.setClusterInfoForJob(null, null, null);
//    }
//
//    /**
//     * Test getting the job status.
//     *
//     * @throws GenieException For any problem
//     */
//    @Test
//    public void testGetJobStatus() throws GenieException {
//        Assert.assertEquals(JobStatus.FAILED, this.service.getJobStatus(JOB_2_ID));
//    }
}
