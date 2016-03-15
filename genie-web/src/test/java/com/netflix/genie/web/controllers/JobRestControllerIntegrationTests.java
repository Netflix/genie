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
package com.netflix.genie.web.controllers;

import com.netflix.genie.common.dto.Application;
import com.netflix.genie.common.dto.ApplicationStatus;
import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.dto.ClusterCriteria;
import com.netflix.genie.common.dto.ClusterStatus;
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.dto.CommandStatus;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.core.jobs.JobConstants;
import com.netflix.genie.core.jpa.repositories.JpaJobExecutionRepository;
import com.netflix.genie.core.jpa.repositories.JpaJobRepository;
import com.netflix.genie.core.jpa.repositories.JpaJobRequestRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.SystemUtils;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.ResourceLoader;
import org.springframework.hateoas.MediaTypes;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Integration tests for Jobs REST API.
 *
 * @author amsharma
 * @since 3.0.0
 */
@Slf4j
public class JobRestControllerIntegrationTests extends RestControllerIntegrationTestsBase {

    protected static final String STATUS_MESSAGE_PATH = "$.statusMsg";
    protected static final String CLUSTER_NAME_PATH = "$.clusterName";
    protected static final String COMMAND_NAME_PATH = "$.commandName";
    protected static final String ARCHIVE_LOCATION_PATH = "$.archiveLocation";
    protected static final String STARTED_PATH = "$.started";
    protected static final String FINISHED_PATH = "$.finished";

    private static final String BASE_DIR
        = "com/netflix/genie/web/controllers/JobRestControllerIntegrationTests/";
    private static final String FILE_DELIMITER = "/";

    private static final String JOB_NAME = "List Directories bash job";
    private static final String JOB_USER = "genie";
    private static final String JOB_VERSION = "1.0";
    private static final String JOB_DESCRIPTION = "Genie 3 Test Job";
    private static final String JOB_STATUS_MSG = "Job finished successfully.";

    private static final String APP1_ID = "app1";
    private static final String APP1_NAME = "Application 1";
    private static final String APP1_USER = "genie";
    private static final String APP1_VERSION = "1.0";

    private static final String APP2_ID = "app2";
    private static final String APP2_NAME = "Application 2";

    private static final String CMD1_ID = "cmd1";
    private static final String CMD1_NAME = "Unix Bash command";
    private static final String CMD1_USER = "genie";
    private static final String CMD1_VERSION = "1.0";

    private static final String CLUSTER1_ID = "cluster1";
    private static final String CLUSTER1_NAME = "Local laptop";
    private static final String CLUSTER1_USER = "genie";
    private static final String CLUSTER1_VERSION = "1.0";

    private ResourceLoader resourceLoader;

    @Autowired
    private JpaJobRepository jobRepository;

    @Autowired
    private JpaJobRequestRepository jobRequestRepository;

    @Autowired
    private JpaJobExecutionRepository jobExecutionRepository;

    @Value("${TRAVIS_PULL_REQUEST:false}")
    private boolean isPullRequest;

//    @Autowired
//    private JpaApplicationRepository applicationRepository;
//
//    @Autowired
//    private JpaCommandRepository commandRepository;
//
//    @Autowired
//    private JpaClusterRepository clusterRepository;

    /**
     * Setup for tests.
     *
     * @throws Exception If there is an error.
     */
    @Before
    public void setup() throws Exception {
        Assume.assumeFalse(this.isPullRequest);
        super.setup();
        this.resourceLoader = new DefaultResourceLoader();
        //this.jobsBaseUrl = "http://localhost:" + this.port + "/api/v3/jobs";
        createAnApplication(APP1_ID, APP1_NAME);
        createAnApplication(APP2_ID, APP2_NAME);
        createAllClusters();
        createAllCommands();
        linkAllEntities();
    }

    private void linkAllEntities(
    ) throws Exception {

        final List<String> apps = new ArrayList<>();
        apps.add(APP1_ID);
        apps.add(APP2_ID);

        this.mvc
            .perform(
                MockMvcRequestBuilders
                    .post(COMMANDS_API + FILE_DELIMITER + CMD1_ID + FILE_DELIMITER + APPLICATIONS_LINK_KEY)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(OBJECT_MAPPER.writeValueAsBytes(apps))
            )
            .andExpect(MockMvcResultMatchers.status().isNoContent());

        final List<String> cmds = new ArrayList<>();
        cmds.add(CMD1_ID);

        this.mvc
            .perform(
                MockMvcRequestBuilders
                    .post(CLUSTERS_API + FILE_DELIMITER + CLUSTER1_ID + FILE_DELIMITER + COMMANDS_LINK_KEY)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(OBJECT_MAPPER.writeValueAsBytes(cmds))
            )
            .andExpect(MockMvcResultMatchers.status().isNoContent());
    }

    private void createAnApplication(
        final String id,
        final String app1Name
    ) throws Exception {

        final String setUpFile = this.resourceLoader.getResource(
            BASE_DIR
                + id
                + FILE_DELIMITER
                + "setupfile"
        ).getFile().getAbsolutePath();

        final Set<String> app1Dependencies = new HashSet<>();
        final String depFile1 = this.resourceLoader.getResource(
            BASE_DIR
                + id
                + FILE_DELIMITER
                + "dep1"
        ).getFile().getAbsolutePath();
        final String depFile2 = this.resourceLoader.getResource(
            BASE_DIR
                + id
                + FILE_DELIMITER
                + "dep2"
        ).getFile().getAbsolutePath();
        app1Dependencies.add(depFile1);
        app1Dependencies.add(depFile2);

        final Set<String> app1Configs = new HashSet<>();
        final String configFile1 = this.resourceLoader.getResource(
            BASE_DIR
                + id
                + FILE_DELIMITER
                + "config1"
        ).getFile().getAbsolutePath();
        final String configFile2 = this.resourceLoader.getResource(
            BASE_DIR
                + id
                + FILE_DELIMITER
                + "config2"
        ).getFile().getAbsolutePath();
        app1Configs.add(configFile1);
        app1Configs.add(configFile2);

        final Application app = new Application.Builder(
            app1Name,
            APP1_USER,
            APP1_VERSION,
            ApplicationStatus.ACTIVE)
            .withId(id)
            .withSetupFile(setUpFile)
            .withConfigs(app1Configs)
            .withDependencies(app1Dependencies)
            .build();

        this.mvc
            .perform(
                MockMvcRequestBuilders
                    .post(APPLICATIONS_API)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(OBJECT_MAPPER.writeValueAsBytes(app))
            )
            .andExpect(MockMvcResultMatchers.status().isCreated())
            .andExpect(MockMvcResultMatchers.header().string(HttpHeaders.LOCATION, Matchers.notNullValue()))
            .andReturn();
    }

    private void createAllClusters() throws Exception {

        final String setUpFile = this.resourceLoader.getResource(
            BASE_DIR
                + CLUSTER1_ID
                + FILE_DELIMITER
                + "setupfile"
        ).getFile().getAbsolutePath();

        final Set<String> configs = new HashSet<>();
        final String configFile1 = this.resourceLoader.getResource(
            BASE_DIR
                + CLUSTER1_ID
                + FILE_DELIMITER
                + "config1"
        ).getFile().getAbsolutePath();
        final String configFile2 = this.resourceLoader.getResource(
            BASE_DIR
                + CLUSTER1_ID
                + FILE_DELIMITER
                + "config2"
        ).getFile().getAbsolutePath();
        configs.add(configFile1);
        configs.add(configFile2);

        final Set<String> tags = new HashSet<>();
        tags.add("localhost");

        final Cluster cluster = new Cluster.Builder(
            CLUSTER1_NAME,
            CLUSTER1_USER,
            CLUSTER1_VERSION,
            ClusterStatus.UP
        )
            .withId(CLUSTER1_ID)
            .withSetupFile(setUpFile)
            .withConfigs(configs)
            .withTags(tags)
            .build();

        this.mvc
            .perform(
                MockMvcRequestBuilders
                    .post(CLUSTERS_API)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(OBJECT_MAPPER.writeValueAsBytes(cluster))
            )
            .andExpect(MockMvcResultMatchers.status().isCreated())
            .andExpect(MockMvcResultMatchers.header().string(HttpHeaders.LOCATION, Matchers.notNullValue()))
            .andReturn();
    }

    private void createAllCommands() throws Exception {

        final String setUpFile = this.resourceLoader.getResource(
            BASE_DIR
                + CMD1_ID
                + FILE_DELIMITER
                + "setupfile"
        ).getFile().getAbsolutePath();

        final Set<String> configs = new HashSet<>();
        final String configFile1 = this.resourceLoader.getResource(
            BASE_DIR
                + CMD1_ID
                + FILE_DELIMITER
                + "config1"
        ).getFile().getAbsolutePath();
        final String configFile2 = this.resourceLoader.getResource(
            BASE_DIR
                + CMD1_ID
                + FILE_DELIMITER
                + "config2"
        ).getFile().getAbsolutePath();
        configs.add(configFile1);
        configs.add(configFile2);

        final Set<String> tags = new HashSet<>();
        tags.add("bash");

        final Command cmd = new Command.Builder(
            CMD1_NAME,
            CMD1_USER,
            CMD1_VERSION,
            CommandStatus.ACTIVE,
            "/bin/bash",
            1
        )
            .withId(CMD1_ID)
            .withSetupFile(setUpFile)
            .withConfigs(configs)
            .withTags(tags)
            .build();

        this.mvc
            .perform(
                MockMvcRequestBuilders
                    .post(COMMANDS_API)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(OBJECT_MAPPER.writeValueAsBytes(cmd))
            )
            .andExpect(MockMvcResultMatchers.status().isCreated())
            .andExpect(MockMvcResultMatchers.header().string(HttpHeaders.LOCATION, Matchers.notNullValue()))
            .andReturn();
    }


//    /**
//     * Cleanup after tests.
//     */
//    @After
//    public void cleanup() {
//        this.jobRequestRepository.deleteAll();
//        this.jobRepository.deleteAll();
//        this.jobExecutionRepository.deleteAll();
//        this.clusterRepository.deleteAll();
//        this.commandRepository.deleteAll();
//        this.applicationRepository.deleteAll();
//    }


    /**
     * Test the job submit method for success.
     *
     * @throws Exception If there is a problem.
     */
    @Test
    @DirtiesContext
    public void testSubmitJobMethod() throws Exception {
        Assume.assumeTrue(SystemUtils.IS_OS_UNIX);
        final String commandArgs = "-c 'echo hello world'";

        final List<ClusterCriteria> clusterCriteriaList = new ArrayList<>();
        final Set<String> clusterTags = new HashSet<>();
        clusterTags.add("localhost");
        final ClusterCriteria clusterCriteria = new ClusterCriteria(clusterTags);
        clusterCriteriaList.add(clusterCriteria);

        final String setUpFile = this.resourceLoader.getResource(
            this.BASE_DIR
                + "job"
                + FILE_DELIMITER
                + "jobsetupfile"
        ).getFile().getAbsolutePath();

        final Set<String> dependencies = new HashSet<>();
        final String depFile1 = this.resourceLoader.getResource(
            this.BASE_DIR
                + "job"
                + FILE_DELIMITER
                + "dep1"
        ).getFile().getAbsolutePath();
//        final String depFile2 = this.resourceLoader.getResource(
//            this.BASE_DIR
//                + "job"
//                + FILE_DELIMITER
//                + "dep2"
//        ).getFile().getAbsolutePath();
        dependencies.add(depFile1);
//        dependencies.add(depFile2);

        final Set<String> commandCriteria = new HashSet<>();
        commandCriteria.add("bash");
        final JobRequest jobRequest = new JobRequest.Builder(
            JOB_NAME,
            JOB_USER,
            JOB_VERSION,
            commandArgs,
            clusterCriteriaList,
            commandCriteria
        )
            .withDisableLogArchival(true)
            .withSetupFile(setUpFile)
            .withFileDependencies(dependencies)
            .withDescription(JOB_DESCRIPTION)
            .build();

        final MvcResult result = this.mvc
            .perform(
                MockMvcRequestBuilders
                    .post(JOBS_API)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(OBJECT_MAPPER.writeValueAsBytes(jobRequest))
            )
            .andExpect(MockMvcResultMatchers.status().isAccepted())
            .andExpect(MockMvcResultMatchers.header().string(HttpHeaders.LOCATION, Matchers.notNullValue()))
            .andReturn();

        final String jobId = this.getIdFromLocation(result.getResponse().getHeader(HttpHeaders.LOCATION));

        int counter = 1;
        do {
            counter++;
            Thread.sleep(1000);
        } while (
            this.mvc.perform(MockMvcRequestBuilders.get(
                JOBS_API
                    + JobConstants.FILE_PATH_DELIMITER
                    + jobId
                    + JobConstants.FILE_PATH_DELIMITER
                    + "status")
                .accept(MediaType.APPLICATION_JSON))
                .andReturn()
                .getResponse()
                .getContentAsString().contains("RUNNING") && counter < 10
            );

        Assert.assertEquals(this.mvc.perform(MockMvcRequestBuilders.get(
            JOBS_API
                + JobConstants.FILE_PATH_DELIMITER
                + jobId
                + JobConstants.FILE_PATH_DELIMITER
                + "status")
            .accept(MediaType.APPLICATION_JSON))
            .andReturn()
            .getResponse()
            .getContentAsString(), "{\"status\":\"SUCCEEDED\"}");

        // TODO check job request and execution once api implemented

        // Check if all the fields are created right in the database
        this.mvc
            .perform(MockMvcRequestBuilders.get(JOBS_API + "/" + jobId))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentType(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.jsonPath(ID_PATH, Matchers.is(jobId)))
            .andExpect(MockMvcResultMatchers.jsonPath(CREATED_PATH, Matchers.notNullValue()))
            .andExpect(MockMvcResultMatchers.jsonPath(UPDATED_PATH, Matchers.notNullValue()))
            .andExpect(MockMvcResultMatchers.jsonPath(VERSION_PATH, Matchers.is(JOB_VERSION)))
            .andExpect(MockMvcResultMatchers.jsonPath(USER_PATH, Matchers.is(JOB_USER)))
            .andExpect(MockMvcResultMatchers.jsonPath(NAME_PATH, Matchers.is(JOB_NAME)))
            .andExpect(MockMvcResultMatchers.jsonPath(DESCRIPTION_PATH, Matchers.is(JOB_DESCRIPTION)))
            .andExpect(MockMvcResultMatchers.jsonPath(STATUS_PATH, Matchers.is(JobStatus.SUCCEEDED.toString())))
            .andExpect(MockMvcResultMatchers.jsonPath(STATUS_MESSAGE_PATH, Matchers.is(JOB_STATUS_MSG)))
            .andExpect(MockMvcResultMatchers.jsonPath(STARTED_PATH, Matchers.not(new Date(0))))
            .andExpect(MockMvcResultMatchers.jsonPath(FINISHED_PATH, Matchers.notNullValue()))
            .andExpect(MockMvcResultMatchers.jsonPath(ARCHIVE_LOCATION_PATH, Matchers.isEmptyOrNullString()))
            .andExpect(MockMvcResultMatchers.jsonPath(CLUSTER_NAME_PATH, Matchers.is(CLUSTER1_NAME)))
            .andExpect(MockMvcResultMatchers.jsonPath(COMMAND_NAME_PATH, Matchers.is(CMD1_NAME)))
            .andExpect(MockMvcResultMatchers.jsonPath(LINKS_PATH + ".*", Matchers.hasSize(4)))
            .andExpect(MockMvcResultMatchers.jsonPath(LINKS_PATH, Matchers.hasKey(SELF_LINK_KEY)));


        // Check the structure of the output directory for the job using the output api
        this.mvc
            .perform(MockMvcRequestBuilders.get(JOBS_API + "/" + jobId + "/output"))
            .andExpect(MockMvcResultMatchers.jsonPath("parent", Matchers.isEmptyOrNullString()))
            .andExpect(MockMvcResultMatchers.jsonPath("$.directories[0].name", Matchers.is("genie/")))
            .andExpect(MockMvcResultMatchers.jsonPath("$.files[0].name", Matchers.is("dep1")))
            .andExpect(MockMvcResultMatchers.jsonPath("$.files[1].name", Matchers.is("jobsetupfile")))
            .andExpect(MockMvcResultMatchers.jsonPath("$.files[2].name", Matchers.is("run.sh")))
            .andExpect(MockMvcResultMatchers.jsonPath("$.files[3].name", Matchers.is("stderr")))
            .andExpect(MockMvcResultMatchers.jsonPath("$.files[4].name", Matchers.is("stdout")));

        Assert.assertThat(this.jobRepository.count(), Matchers.is(1L));
        Assert.assertThat(this.jobRequestRepository.count(), Matchers.is(1L));
        Assert.assertThat(this.jobExecutionRepository.count(), Matchers.is(1L));
    }

    /**
     * Test the job submit method for job already exists.
     *
     * @throws Exception If there is a problem.
     */
    @Test
    @DirtiesContext
    public void testSubmitJobMethodAlreadyExists() throws Exception {
        Assume.assumeTrue(SystemUtils.IS_OS_UNIX);
        final String commandArgs = "-c 'echo hello world'";

        final List<ClusterCriteria> clusterCriteriaList = new ArrayList<>();
        final Set<String> clusterTags = new HashSet<>();
        clusterTags.add("localhost");
        final ClusterCriteria clusterCriteria = new ClusterCriteria(clusterTags);
        clusterCriteriaList.add(clusterCriteria);

        final String jobId = UUID.randomUUID().toString();

        final Set<String> commandCriteria = new HashSet<>();
        commandCriteria.add("bash");
        final JobRequest jobRequest = new JobRequest.Builder(
            JOB_NAME,
            JOB_USER,
            JOB_VERSION,
            commandArgs,
            clusterCriteriaList,
            commandCriteria
        )
            .withId(jobId)
            .withDisableLogArchival(true)
            .build();

        this.mvc
            .perform(
                MockMvcRequestBuilders
                    .post(JOBS_API)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(OBJECT_MAPPER.writeValueAsBytes(jobRequest))
            );

        this.mvc
            .perform(
                MockMvcRequestBuilders
                    .post(JOBS_API)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(OBJECT_MAPPER.writeValueAsBytes(jobRequest))
            )
            .andExpect(MockMvcResultMatchers.status().isConflict());

        int counter = 1;
        do {
            counter++;
            Thread.sleep(1000);
        } while (
            this.mvc.perform(MockMvcRequestBuilders.get(
                JOBS_API
                    + JobConstants.FILE_PATH_DELIMITER
                    + jobId
                    + JobConstants.FILE_PATH_DELIMITER
                    + "status")
                .accept(MediaType.APPLICATION_JSON))
                .andReturn()
                .getResponse()
                .getContentAsString().contains("RUNNING") && counter < 10
            );

        Assert.assertEquals(this.mvc.perform(MockMvcRequestBuilders.get(
            JOBS_API
                + JobConstants.FILE_PATH_DELIMITER
                + jobId
                + JobConstants.FILE_PATH_DELIMITER
                + "status")
            .accept(MediaType.APPLICATION_JSON))
            .andReturn()
            .getResponse()
            .getContentAsString(), "{\"status\":\"SUCCEEDED\"}");
    }

    /**
     * Test the job submit method for incorrect cluster resolved.
     *
     * @throws Exception If there is a problem.
     */
    @Test
    @DirtiesContext
    public void testSubmitJobMethodMissingCluster() throws Exception {
        Assume.assumeTrue(SystemUtils.IS_OS_UNIX);
        final String commandArgs = "-c 'echo hello world'";

        final List<ClusterCriteria> clusterCriteriaList = new ArrayList<>();
        final Set<String> clusterTags = new HashSet<>();
        clusterTags.add("undefined");
        final ClusterCriteria clusterCriteria = new ClusterCriteria(clusterTags);
        clusterCriteriaList.add(clusterCriteria);

        final String jobId = UUID.randomUUID().toString();

        final Set<String> commandCriteria = new HashSet<>();
        commandCriteria.add("bash");
        final JobRequest jobRequest = new JobRequest.Builder(
            JOB_NAME,
            JOB_USER,
            JOB_VERSION,
            commandArgs,
            clusterCriteriaList,
            commandCriteria
        )
            .withId(jobId)
            .withDisableLogArchival(true)
            .build();

        this.mvc
            .perform(
                MockMvcRequestBuilders
                    .post(JOBS_API)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(OBJECT_MAPPER.writeValueAsBytes(jobRequest))
                    .accept(MediaType.APPLICATION_JSON)
            )
            .andExpect(MockMvcResultMatchers.status().isPreconditionFailed());
    }

    /**
     * Test the job submit method for incorrect command resolved.
     *
     * @throws Exception If there is a problem.
     */
    @Test
    @DirtiesContext
    public void testSubmitJobMethodMissingCommand() throws Exception {
        Assume.assumeTrue(SystemUtils.IS_OS_UNIX);
        final String commandArgs = "-c 'echo hello world'";

        final List<ClusterCriteria> clusterCriteriaList = new ArrayList<>();
        final Set<String> clusterTags = new HashSet<>();
        clusterTags.add("localhost");
        final ClusterCriteria clusterCriteria = new ClusterCriteria(clusterTags);
        clusterCriteriaList.add(clusterCriteria);

        final String jobId = UUID.randomUUID().toString();

        final Set<String> commandCriteria = new HashSet<>();
        commandCriteria.add("undefined");
        final JobRequest jobRequest = new JobRequest.Builder(
            JOB_NAME,
            JOB_USER,
            JOB_VERSION,
            commandArgs,
            clusterCriteriaList,
            commandCriteria
        )
            .withId(jobId)
            .withDisableLogArchival(true)
            .build();

        this.mvc
            .perform(
                MockMvcRequestBuilders
                    .post(JOBS_API)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(OBJECT_MAPPER.writeValueAsBytes(jobRequest))
            )
            .andExpect(MockMvcResultMatchers.status().isPreconditionFailed());
    }

    /**
     * Test the job submit method for when the job is killed.
     *
     * @throws Exception If there is a problem.
     */
//    @Test
//    public void testSubmitJobMethodKill() throws Exception {
//        Assume.assumeTrue(SystemUtils.IS_OS_UNIX);
//        final String commandArgs = "-c 'ls foo'";
//
//        final List<ClusterCriteria> clusterCriteriaList = new ArrayList<>();
//        final Set<String> clusterTags = new HashSet<>();
//        clusterTags.add("localhost");
//        final ClusterCriteria clusterCriteria = new ClusterCriteria(clusterTags);
//        clusterCriteriaList.add(clusterCriteria);
//
//        final Set<String> commandCriteria = new HashSet<>();
//        commandCriteria.add("bash");
//        final JobRequest jobRequest = new JobRequest.Builder(
//            JOB_NAME,
//            JOB_USER,
//            JOB_VERSION,
//            commandArgs,
//            clusterCriteriaList,
//            commandCriteria
//        )
//            .withDisableLogArchival(true)
//            .build();
//
//        final MvcResult result = this.mvc
//            .perform(
//                MockMvcRequestBuilders
//                    .post(JOBS_API)
//                    .contentType(MediaType.APPLICATION_JSON)
//                    .content(OBJECT_MAPPER.writeValueAsBytes(jobRequest))
//            )
//            .andExpect(MockMvcResultMatchers.status().isAccepted())
//            .andExpect(MockMvcResultMatchers.header().string(HttpHeaders.LOCATION, Matchers.notNullValue()))
//            .andReturn();
//
//        final String jobId = this.getIdFromLocation(result.getResponse().getHeader(HttpHeaders.LOCATION));
//
//        // Send a kill request to the job.
//        this.mvc
//            .perform(
//                MockMvcRequestBuilders
//                    .delete(JOBS_API + "/" + jobId)
//            );
//
//        try {
//            Thread.sleep(1000);
//        } catch (InterruptedException ie) {
//            //Handle exception
//        }
//
//        this.mvc
//            .perform(MockMvcRequestBuilders.get(JOBS_API + "/" + jobId))
//            .andExpect(MockMvcResultMatchers.status().isOk())
//            .andExpect(MockMvcResultMatchers.content().contentType(MediaTypes.HAL_JSON))
//            .andExpect(MockMvcResultMatchers.jsonPath(ID_PATH, Matchers.is(jobId)))
//            .andExpect(MockMvcResultMatchers.jsonPath(STATUS_PATH, Matchers.is(JobStatus.KILLED.toString())));
//    }

    /**
     * Test the job submit method for when the job fails.
     *
     * @throws Exception If there is a problem.
     */
    @Test
    @DirtiesContext
    public void testSubmitJobMethodFailure() throws Exception {
        Assume.assumeTrue(SystemUtils.IS_OS_UNIX);
        final String commandArgs = "-c 'exit 1'";

        final List<ClusterCriteria> clusterCriteriaList = new ArrayList<>();
        final Set<String> clusterTags = new HashSet<>();
        clusterTags.add("localhost");
        final ClusterCriteria clusterCriteria = new ClusterCriteria(clusterTags);
        clusterCriteriaList.add(clusterCriteria);

        final Set<String> commandCriteria = new HashSet<>();
        commandCriteria.add("bash");
        final JobRequest jobRequest = new JobRequest.Builder(
            JOB_NAME,
            JOB_USER,
            JOB_VERSION,
            commandArgs,
            clusterCriteriaList,
            commandCriteria
        )
            .withDisableLogArchival(true)
            .build();

        final MvcResult result = this.mvc
            .perform(
                MockMvcRequestBuilders
                    .post(JOBS_API)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(OBJECT_MAPPER.writeValueAsBytes(jobRequest))
            )
            .andExpect(MockMvcResultMatchers.status().isAccepted())
            .andExpect(MockMvcResultMatchers.header().string(HttpHeaders.LOCATION, Matchers.notNullValue()))
            .andReturn();

        final String jobId = this.getIdFromLocation(result.getResponse().getHeader(HttpHeaders.LOCATION));
        int counter = 1;
        do {
            counter++;
            Thread.sleep(1000);
        } while (
            this.mvc.perform(MockMvcRequestBuilders.get(
                JOBS_API
                    + JobConstants.FILE_PATH_DELIMITER
                    + jobId
                    + JobConstants.FILE_PATH_DELIMITER
                    + "status")
                .accept(MediaType.APPLICATION_JSON))
                .andReturn()
                .getResponse()
                .getContentAsString().contains("RUNNING") && counter < 10
            );

        Assert.assertEquals(this.mvc.perform(MockMvcRequestBuilders.get(
            JOBS_API
                + JobConstants.FILE_PATH_DELIMITER
                + jobId
                + JobConstants.FILE_PATH_DELIMITER
                + "status")
            .accept(MediaType.APPLICATION_JSON))
            .andReturn()
            .getResponse()
            .getContentAsString(), "{\"status\":\"FAILED\"}");

        this.mvc
            .perform(MockMvcRequestBuilders.get(JOBS_API + "/" + jobId))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentType(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.jsonPath(ID_PATH, Matchers.is(jobId)))
            .andExpect(MockMvcResultMatchers.jsonPath(STATUS_PATH, Matchers.is(JobStatus.FAILED.toString())));
    }

    private String getIdFromLocation(final String location) {
        return location.substring(location.lastIndexOf("/") + 1);
    }
}
