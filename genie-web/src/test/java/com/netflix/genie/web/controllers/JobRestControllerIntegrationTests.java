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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.Application;
import com.netflix.genie.common.dto.ApplicationStatus;
import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.dto.ClusterCriteria;
import com.netflix.genie.common.dto.ClusterStatus;
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.dto.CommandStatus;
import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.dto.JobStatusMessages;
import com.netflix.genie.core.jpa.repositories.JpaApplicationRepository;
import com.netflix.genie.core.jpa.repositories.JpaClusterRepository;
import com.netflix.genie.core.jpa.repositories.JpaCommandRepository;
import com.netflix.genie.core.jpa.repositories.JpaJobExecutionRepository;
import com.netflix.genie.core.jpa.repositories.JpaJobMetadataRepository;
import com.netflix.genie.core.jpa.repositories.JpaJobRepository;
import com.netflix.genie.core.jpa.repositories.JpaJobRequestRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.SystemUtils;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.hateoas.MediaTypes;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.restdocs.headers.HeaderDocumentation;
import org.springframework.restdocs.mockmvc.MockMvcRestDocumentation;
import org.springframework.restdocs.mockmvc.RestDocumentationRequestBuilders;
import org.springframework.restdocs.mockmvc.RestDocumentationResultHandler;
import org.springframework.restdocs.operation.preprocess.Preprocessors;
import org.springframework.restdocs.payload.PayloadDocumentation;
import org.springframework.restdocs.request.RequestDocumentation;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMultipartHttpServletRequestBuilder;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Integration tests for Jobs REST API.
 *
 * @author amsharma
 * @author tgianos
 * @since 3.0.0
 */
@Slf4j
public class JobRestControllerIntegrationTests extends RestControllerIntegrationTestsBase {

    private static final long SLEEP_TIME = 1000L;

    private static final String COMMAND_ARGS_PATH = "$.commandArgs";
    private static final String STATUS_MESSAGE_PATH = "$.statusMsg";
    private static final String CLUSTER_NAME_PATH = "$.clusterName";
    private static final String COMMAND_NAME_PATH = "$.commandName";
    private static final String ARCHIVE_LOCATION_PATH = "$.archiveLocation";
    private static final String STARTED_PATH = "$.started";
    private static final String FINISHED_PATH = "$.finished";
    private static final String CLUSTER_CRITERIAS_PATH = "$.clusterCriterias";
    private static final String COMMAND_CRITERIA_PATH = "$.commandCriteria";
    private static final String GROUP_PATH = "$.group";
    private static final String DISABLE_LOG_ARCHIVAL_PATH = "$.disableLogArchival";
    private static final String EMAIL_PATH = "$.email";
    private static final String CPU_PATH = "$.cpu";
    private static final String MEMORY_PATH = "$.memory";
    private static final String DEPENDENCIES_PATH = "$.dependencies";
    private static final String APPLICATIONS_PATH = "$.applications";
    private static final String HOST_NAME_PATH = "$.hostName";
    private static final String PROCESS_ID_PATH = "$.processId";
    private static final String CHECK_DELAY_PATH = "$.checkDelay";
    private static final String EXIT_CODE_PATH = "$.exitCode";

    private static final long CHECK_DELAY = 1L;

    private static final String BASE_DIR
        = "com/netflix/genie/web/controllers/JobRestControllerIntegrationTests/";
    private static final String FILE_DELIMITER = "/";

    private static final String JOB_NAME = "List * ... Directories bash job";
    private static final String JOB_USER = "genie";
    private static final String JOB_VERSION = "1.0";
    private static final String JOB_DESCRIPTION = "Genie 3 Test Job";
    private static final String JOB_STATUS_MSG = JobStatusMessages.JOB_FINISHED_SUCCESSFULLY;

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
    private static final String CMD1_EXECUTABLE = "/bin/bash";

    private static final String CLUSTER1_ID = "cluster1";
    private static final String CLUSTER1_NAME = "Local laptop";
    private static final String CLUSTER1_USER = "genie";
    private static final String CLUSTER1_VERSION = "1.0";

    private static final String JOBS_LIST_PATH = EMBEDDED_PATH + ".jobSearchResultList";

    private ResourceLoader resourceLoader;

    @Autowired
    private JpaJobRepository jobRepository;

    @Autowired
    private JpaJobRequestRepository jobRequestRepository;

    @Autowired
    private JpaJobMetadataRepository jobRequestMetadataRepository;

    @Autowired
    private JpaJobExecutionRepository jobExecutionRepository;

    @Autowired
    private JpaApplicationRepository applicationRepository;

    @Autowired
    private JpaCommandRepository commandRepository;

    @Autowired
    private JpaClusterRepository clusterRepository;

    @Autowired
    private String hostname;

    @Autowired
    private Resource jobDirResource;

    @Value("${genie.file.cache.location}")
    private String baseCacheLocation;

    /**
     * Setup for tests.
     *
     * @throws Exception If there is an error.
     */
    @Before
    public void setup() throws Exception {
        super.setup();
        this.resourceLoader = new DefaultResourceLoader();
        //this.jobsBaseUrl = "http://localhost:" + this.port + "/api/v3/jobs";
        createAnApplication(APP1_ID, APP1_NAME);
        createAnApplication(APP2_ID, APP2_NAME);
        createAllClusters();
        createAllCommands();
        linkAllEntities();
    }

    private void linkAllEntities() throws Exception {
        final List<String> apps = new ArrayList<>();
        apps.add(APP1_ID);
        apps.add(APP2_ID);

        this.mvc
            .perform(
                MockMvcRequestBuilders
                    .post(COMMANDS_API + FILE_DELIMITER + CMD1_ID + FILE_DELIMITER + APPLICATIONS_LINK_KEY)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(objectMapper.writeValueAsBytes(apps))
            )
            .andExpect(MockMvcResultMatchers.status().isNoContent());

        final List<String> cmds = Lists.newArrayList(CMD1_ID);

        this.mvc
            .perform(
                MockMvcRequestBuilders
                    .post(CLUSTERS_API + FILE_DELIMITER + CLUSTER1_ID + FILE_DELIMITER + COMMANDS_LINK_KEY)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(this.objectMapper.writeValueAsBytes(cmds))
            )
            .andExpect(MockMvcResultMatchers.status().isNoContent());
    }

    private void createAnApplication(final String id, final String appName) throws Exception {
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
            appName,
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
                    .content(this.objectMapper.writeValueAsBytes(app))
            )
            .andExpect(MockMvcResultMatchers.status().isCreated())
            .andExpect(MockMvcResultMatchers.header().string(HttpHeaders.LOCATION, Matchers.notNullValue()));
    }

    private void createAllClusters() throws Exception {
        final String setUpFile = this.resourceLoader.getResource(
            BASE_DIR + CLUSTER1_ID + FILE_DELIMITER + "setupfile"
        ).getFile().getAbsolutePath();

        final Set<String> configs = new HashSet<>();
        final String configFile1 = this.resourceLoader.getResource(
            BASE_DIR + CLUSTER1_ID + FILE_DELIMITER + "config1"
        ).getFile().getAbsolutePath();
        final String configFile2 = this.resourceLoader.getResource(
            BASE_DIR + CLUSTER1_ID + FILE_DELIMITER + "config2"
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
                    .content(objectMapper.writeValueAsBytes(cluster))
            )
            .andExpect(MockMvcResultMatchers.status().isCreated())
            .andExpect(MockMvcResultMatchers.header().string(HttpHeaders.LOCATION, Matchers.notNullValue()));
    }

    private void createAllCommands() throws Exception {
        final String setUpFile = this.resourceLoader.getResource(
            BASE_DIR + CMD1_ID + FILE_DELIMITER + "setupfile"
        ).getFile().getAbsolutePath();

        final Set<String> configs = new HashSet<>();
        final String configFile1 = this.resourceLoader.getResource(
            BASE_DIR + CMD1_ID + FILE_DELIMITER + "config1"
        ).getFile().getAbsolutePath();
        final String configFile2 = this.resourceLoader.getResource(
            BASE_DIR + CMD1_ID + FILE_DELIMITER + "config2"
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
            CMD1_EXECUTABLE,
            CHECK_DELAY
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
                    .content(objectMapper.writeValueAsBytes(cmd))
            )
            .andExpect(MockMvcResultMatchers.status().isCreated())
            .andExpect(MockMvcResultMatchers.header().string(HttpHeaders.LOCATION, Matchers.notNullValue()));
    }

    /**
     * Cleanup after tests.
     *
     * @throws Exception who cares
     */
    @After
    public void cleanup() throws Exception {
        this.jobRequestMetadataRepository.deleteAll();
        this.jobExecutionRepository.deleteAll();
        this.jobRepository.deleteAll();
        this.jobRequestRepository.deleteAll();
        this.clusterRepository.deleteAll();
        this.commandRepository.deleteAll();
        this.applicationRepository.deleteAll();
    }

    /**
     * Test the job submit method for success.
     *
     * @throws Exception If there is a problem.
     */
    @Test
    public void testSubmitJobMethodSuccess() throws Exception {
        this.submitAndCheckJob(1);
    }

    private void submitAndCheckJob(final int documentationId) throws Exception {
        Assume.assumeTrue(SystemUtils.IS_OS_UNIX);
        final String commandArgs = "-c 'echo hello world'";

        final String clusterTag = "localhost";
        final List<ClusterCriteria> clusterCriteriaList = Lists.newArrayList(
            new ClusterCriteria(Sets.newHashSet(clusterTag))
        );

        final String setUpFile = this.resourceLoader
            .getResource(BASE_DIR + "job" + FILE_DELIMITER + "jobsetupfile")
            .getFile()
            .getAbsolutePath();

        final String depFile1 = this.resourceLoader
            .getResource(BASE_DIR + "job" + FILE_DELIMITER + "dep1")
            .getFile()
            .getAbsolutePath();
        final Set<String> dependencies = Sets.newHashSet(depFile1);

        final String commandTag = "bash";
        final Set<String> commandCriteria = Sets.newHashSet(commandTag);
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
            .withDependencies(dependencies)
            .withDescription(JOB_DESCRIPTION)
            .build();

        final String id = this.submitJob(documentationId, jobRequest, null);
        this.waitForDone(id);

        this.checkJobStatus(documentationId, id);
        this.checkJob(documentationId, id, commandArgs);
        this.checkJobOutput(documentationId, id);
        this.checkJobRequest(documentationId, id, commandArgs, setUpFile, clusterTag, commandTag, depFile1);
        this.checkJobExecution(documentationId, id);
        this.checkJobCluster(documentationId, id);
        this.checkJobCommand(documentationId, id);
        this.checkJobApplications(documentationId, id);
        this.checkFindJobs(documentationId, id, JOB_USER);

        Assert.assertThat(this.jobRepository.count(), Matchers.is(1L));
        Assert.assertThat(this.jobRequestRepository.count(), Matchers.is(1L));
        Assert.assertThat(this.jobRequestMetadataRepository.count(), Matchers.is(1L));
        Assert.assertThat(this.jobExecutionRepository.count(), Matchers.is(1L));

        // Check if the cluster setup file is cached
        final String clusterSetUpFilePath = this.resourceLoader
            .getResource(BASE_DIR + CMD1_ID + FILE_DELIMITER + "setupfile")
            .getFile()
            .getAbsolutePath();
        Assert.assertTrue(
            Files.exists(
                Paths.get(
                    new URI(this.baseCacheLocation).getPath(),
                    UUID.nameUUIDFromBytes(clusterSetUpFilePath.getBytes(Charset.forName("UTF-8"))).toString()
                )
            )
        );
        // Test for conflicts
        this.testForConflicts(id, commandArgs, clusterCriteriaList, commandCriteria);
    }

    private String submitJob(
        final int documentationId,
        final JobRequest jobRequest,
        final List<MockMultipartFile> attachments
    ) throws Exception {
        final MvcResult result;

        if (attachments != null) {
            final RestDocumentationResultHandler createResultHandler = MockMvcRestDocumentation.document(
                "{class-name}/" + documentationId + "/submitJobWithAttachments/",
                Preprocessors.preprocessRequest(Preprocessors.prettyPrint()),
                Preprocessors.preprocessResponse(Preprocessors.prettyPrint()),
                HeaderDocumentation.requestHeaders(
                    HeaderDocumentation
                        .headerWithName(HttpHeaders.CONTENT_TYPE)
                        .description(MediaType.MULTIPART_FORM_DATA_VALUE)
                ), // Request headers
                RequestDocumentation.requestParts(
                    RequestDocumentation
                        .partWithName("request")
                        .description("The job request JSON. Content type must be application/json for part"),
                    RequestDocumentation
                        .partWithName("attachment")
                        .description("An attachment file. There can be multiple. Type should be octet-stream")
                ), // Request parts
                Snippets.LOCATION_HEADER // Response Headers
            );

            final MockMultipartFile json = new MockMultipartFile(
                "request",
                "",
                MediaType.APPLICATION_JSON_VALUE,
                this.objectMapper.writeValueAsBytes(jobRequest)
            );

            final MockMultipartHttpServletRequestBuilder builder = RestDocumentationRequestBuilders
                .fileUpload(JOBS_API)
                .file(json);

            for (final MockMultipartFile attachment : attachments) {
                builder.file(attachment);
            }

            builder.contentType(MediaType.MULTIPART_FORM_DATA);
            result = this.mvc
                .perform(builder)
                .andExpect(MockMvcResultMatchers.status().isAccepted())
                .andExpect(MockMvcResultMatchers.header().string(HttpHeaders.LOCATION, Matchers.notNullValue()))
                .andDo(createResultHandler)
                .andReturn();
        } else {
            // Use regular POST
            final RestDocumentationResultHandler createResultHandler = MockMvcRestDocumentation.document(
                "{class-name}/" + documentationId + "/submitJobWithoutAttachments/",
                Preprocessors.preprocessRequest(Preprocessors.prettyPrint()),
                Preprocessors.preprocessResponse(Preprocessors.prettyPrint()),
                Snippets.CONTENT_TYPE_HEADER, // Request headers
                Snippets.getJobRequestRequestPayload(), // Request Fields
                Snippets.LOCATION_HEADER // Response Headers
            );

            result = this.mvc
                .perform(
                    MockMvcRequestBuilders
                        .post(JOBS_API)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(this.objectMapper.writeValueAsBytes(jobRequest))
                )
                .andExpect(MockMvcResultMatchers.status().isAccepted())
                .andExpect(MockMvcResultMatchers.header().string(HttpHeaders.LOCATION, Matchers.notNullValue()))
                .andDo(createResultHandler)
                .andReturn();
        }

        return this.getIdFromLocation(result.getResponse().getHeader(HttpHeaders.LOCATION));
    }

    private void checkJobStatus(final int documentationId, final String id) throws Exception {
        final RestDocumentationResultHandler getResultHandler = MockMvcRestDocumentation.document(
            "{class-name}/" + documentationId + "/getJobStatus/",
            Preprocessors.preprocessRequest(Preprocessors.prettyPrint()),
            Preprocessors.preprocessResponse(Preprocessors.prettyPrint()),
            Snippets.ID_PATH_PARAM, // Path parameters
            Snippets.JSON_CONTENT_TYPE_HEADER, // Response Headers
            PayloadDocumentation.responseFields(
                PayloadDocumentation
                    .fieldWithPath("status")
                    .description("The job status. One of: " + Arrays.toString(JobStatus.values()))
                    .attributes(Snippets.EMPTY_CONSTRAINTS)
            ) // Response fields
        );

        this.mvc
            .perform(RestDocumentationRequestBuilders.get(JOBS_API + "/{id}/status", id))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(MockMvcResultMatchers.jsonPath(STATUS_PATH, Matchers.is(JobStatus.SUCCEEDED.toString())))
            .andDo(getResultHandler);
    }

    private void checkJob(final int documentationId, final String id, final String commandArgs) throws Exception {
        final RestDocumentationResultHandler getResultHandler = MockMvcRestDocumentation.document(
            "{class-name}/" + documentationId + "/getJob/",
            Preprocessors.preprocessRequest(Preprocessors.prettyPrint()),
            Preprocessors.preprocessResponse(Preprocessors.prettyPrint()),
            Snippets.ID_PATH_PARAM, // Path parameters
            Snippets.HAL_CONTENT_TYPE_HEADER, // Response Headers
            Snippets.getJobResponsePayload(), // Response fields
            Snippets.JOB_LINKS // Links
        );
        this.mvc
            .perform(RestDocumentationRequestBuilders.get(JOBS_API + "/{id}", id))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentType(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.jsonPath(ID_PATH, Matchers.is(id)))
            .andExpect(MockMvcResultMatchers.jsonPath(CREATED_PATH, Matchers.notNullValue()))
            .andExpect(MockMvcResultMatchers.jsonPath(UPDATED_PATH, Matchers.notNullValue()))
            .andExpect(MockMvcResultMatchers.jsonPath(VERSION_PATH, Matchers.is(JOB_VERSION)))
            .andExpect(MockMvcResultMatchers.jsonPath(USER_PATH, Matchers.is(JOB_USER)))
            .andExpect(MockMvcResultMatchers.jsonPath(NAME_PATH, Matchers.is(JOB_NAME)))
            .andExpect(MockMvcResultMatchers.jsonPath(DESCRIPTION_PATH, Matchers.is(JOB_DESCRIPTION)))
            .andExpect(MockMvcResultMatchers.jsonPath(COMMAND_ARGS_PATH, Matchers.is(commandArgs)))
            .andExpect(MockMvcResultMatchers.jsonPath(STATUS_PATH, Matchers.is(JobStatus.SUCCEEDED.toString())))
            .andExpect(MockMvcResultMatchers.jsonPath(STATUS_MESSAGE_PATH, Matchers.is(JOB_STATUS_MSG)))
            .andExpect(MockMvcResultMatchers.jsonPath(STARTED_PATH, Matchers.not(new Date(0))))
            .andExpect(MockMvcResultMatchers.jsonPath(FINISHED_PATH, Matchers.notNullValue()))
            .andExpect(MockMvcResultMatchers.jsonPath(ARCHIVE_LOCATION_PATH, Matchers.isEmptyOrNullString()))
            .andExpect(MockMvcResultMatchers.jsonPath(CLUSTER_NAME_PATH, Matchers.is(CLUSTER1_NAME)))
            .andExpect(MockMvcResultMatchers.jsonPath(COMMAND_NAME_PATH, Matchers.is(CMD1_NAME)))
            .andExpect(MockMvcResultMatchers.jsonPath(LINKS_PATH + ".*", Matchers.hasSize(8)))
            .andExpect(MockMvcResultMatchers.jsonPath(LINKS_PATH, Matchers.hasKey(SELF_LINK_KEY)))
            .andExpect(MockMvcResultMatchers.jsonPath(LINKS_PATH, Matchers.hasKey("request")))
            .andExpect(MockMvcResultMatchers.jsonPath(LINKS_PATH, Matchers.hasKey("execution")))
            .andExpect(MockMvcResultMatchers.jsonPath(LINKS_PATH, Matchers.hasKey("output")))
            .andExpect(MockMvcResultMatchers.jsonPath(LINKS_PATH, Matchers.hasKey("status")))
            .andExpect(MockMvcResultMatchers.jsonPath(LINKS_PATH, Matchers.hasKey("cluster")))
            .andExpect(MockMvcResultMatchers.jsonPath(LINKS_PATH, Matchers.hasKey("command")))
            .andExpect(MockMvcResultMatchers.jsonPath(LINKS_PATH, Matchers.hasKey("applications")))
            .andDo(getResultHandler);
    }

    private void checkJobOutput(final int documentationId, final String id) throws Exception {
        // Check getting a directory as json
        final RestDocumentationResultHandler jsonResultHandler = MockMvcRestDocumentation.document(
            "{class-name}/" + documentationId + "/getJobOutput/json/",
            Preprocessors.preprocessRequest(Preprocessors.prettyPrint()),
            Preprocessors.preprocessResponse(Preprocessors.prettyPrint()),
            Snippets.ID_PATH_PARAM.and(
                RequestDocumentation
                    .parameterWithName("filePath")
                    .description("The path to the directory to get")
                    .optional()
            ), // Path parameters
            HeaderDocumentation.requestHeaders(
                HeaderDocumentation
                    .headerWithName(HttpHeaders.ACCEPT)
                    .description(MediaType.APPLICATION_JSON_VALUE)
                    .optional()
            ), // Request header
            HeaderDocumentation.responseHeaders(
                HeaderDocumentation
                    .headerWithName(HttpHeaders.CONTENT_TYPE)
                    .description(MediaType.APPLICATION_JSON_VALUE)
            ), // Response Headers
            Snippets.OUTPUT_DIRECTORY_FIELDS
        );
        this.mvc
            .perform(RestDocumentationRequestBuilders.get(JOBS_API + "/{id}/output/{filePath}", id, ""))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(MockMvcResultMatchers.jsonPath("parent", Matchers.isEmptyOrNullString()))
            .andExpect(MockMvcResultMatchers.jsonPath("$.directories[0].name", Matchers.is("genie/")))
            .andExpect(MockMvcResultMatchers.jsonPath("$.files[0].name", Matchers.is("dep1")))
            .andExpect(MockMvcResultMatchers.jsonPath("$.files[1].name", Matchers.is("jobsetupfile")))
            .andExpect(MockMvcResultMatchers.jsonPath("$.files[2].name", Matchers.is("run")))
            .andExpect(MockMvcResultMatchers.jsonPath("$.files[3].name", Matchers.is("stderr")))
            .andExpect(MockMvcResultMatchers.jsonPath("$.files[4].name", Matchers.is("stdout")))
            .andDo(jsonResultHandler);

        // Check getting a directory as HTML
        final RestDocumentationResultHandler htmlResultHandler = MockMvcRestDocumentation.document(
            "{class-name}/" + documentationId + "/getJobOutput/html/",
            Preprocessors.preprocessRequest(Preprocessors.prettyPrint()),
            Preprocessors.preprocessResponse(Preprocessors.prettyPrint()),
            Snippets.ID_PATH_PARAM.and(
                RequestDocumentation
                    .parameterWithName("filePath")
                    .description("The path to the directory to get")
                    .optional()
            ), // Path parameters
            HeaderDocumentation.requestHeaders(
                HeaderDocumentation
                    .headerWithName(HttpHeaders.ACCEPT)
                    .description(MediaType.TEXT_HTML)
            ), // Request header
            HeaderDocumentation.responseHeaders(
                HeaderDocumentation
                    .headerWithName(HttpHeaders.CONTENT_TYPE)
                    .description(MediaType.TEXT_HTML)
            ) // Response Headers
        );
        this.mvc
            .perform(RestDocumentationRequestBuilders
                .get(JOBS_API + "/{id}/output/{filePath}", id, "")
                .accept(MediaType.TEXT_HTML)
            )
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentType(MediaType.TEXT_HTML))
            .andDo(htmlResultHandler);

        // Check getting a file

        // Check getting a directory as HTML
        final RestDocumentationResultHandler fileResultHandler = MockMvcRestDocumentation.document(
            "{class-name}/" + documentationId + "/getJobOutput/file/",
            Preprocessors.preprocessRequest(Preprocessors.prettyPrint()),
            Preprocessors.preprocessResponse(Preprocessors.prettyPrint()),
            Snippets.ID_PATH_PARAM.and(
                RequestDocumentation
                    .parameterWithName("filePath")
                    .description("The path to the file to get")
                    .optional()
            ), // Path parameters
            HeaderDocumentation.requestHeaders(
                HeaderDocumentation
                    .headerWithName(HttpHeaders.ACCEPT)
                    .description(MediaType.ALL_VALUE)
                    .optional()
            ), // Request header
            HeaderDocumentation.responseHeaders(
                HeaderDocumentation
                    .headerWithName(HttpHeaders.CONTENT_TYPE)
                    .description("The content type of the file being returned")
                    .optional()
            ) // Response Headers
        );

        // check that the generated run file is correct
        final String runShFileName = SystemUtils.IS_OS_LINUX ? "linux-runsh.txt" : "non-linux-runsh.txt";

        final String runShFile = this.resourceLoader
            .getResource(BASE_DIR + runShFileName)
            .getFile()
            .getAbsolutePath();
        final String runFileContents = new String(Files.readAllBytes(Paths.get(runShFile)), "UTF-8");

        final String jobWorkingDir = this.jobDirResource.getFile().getCanonicalPath() + FILE_DELIMITER + id;
        final String expectedRunScriptContent = this.getExpectedRunContents(runFileContents, jobWorkingDir, id);

        this.mvc
            .perform(RestDocumentationRequestBuilders
                .get(JOBS_API + "/{id}/output/{filePath}", id, "run")
            )
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().string(expectedRunScriptContent))
            .andDo(fileResultHandler);
    }

    private void checkJobRequest(
        final int documentationId,
        final String id,
        final String commandArgs,
        final String setupFile,
        final String clusterTag,
        final String commandTag,
        final String depFile1
    ) throws Exception {
        final RestDocumentationResultHandler getResultHandler = MockMvcRestDocumentation.document(
            "{class-name}/" + documentationId + "/getJobRequest/",
            Preprocessors.preprocessRequest(Preprocessors.prettyPrint()),
            Preprocessors.preprocessResponse(Preprocessors.prettyPrint()),
            Snippets.ID_PATH_PARAM, // Path parameters
            Snippets.HAL_CONTENT_TYPE_HEADER, // Response Headers
            Snippets.getJobRequestResponsePayload(), // Response fields
            Snippets.JOB_REQUEST_LINKS // Links
        );
        this.mvc
            .perform(RestDocumentationRequestBuilders.get(JOBS_API + "/{id}/request", id))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.jsonPath(ID_PATH, Matchers.is(id)))
            .andExpect(MockMvcResultMatchers.jsonPath(CREATED_PATH, Matchers.notNullValue()))
            .andExpect(MockMvcResultMatchers.jsonPath(UPDATED_PATH, Matchers.notNullValue()))
            .andExpect(MockMvcResultMatchers.jsonPath(NAME_PATH, Matchers.is(JOB_NAME)))
            .andExpect(MockMvcResultMatchers.jsonPath(VERSION_PATH, Matchers.is(JOB_VERSION)))
            .andExpect(MockMvcResultMatchers.jsonPath(USER_PATH, Matchers.is(JOB_USER)))
            .andExpect(MockMvcResultMatchers.jsonPath(DESCRIPTION_PATH, Matchers.is(JOB_DESCRIPTION)))
            .andExpect(MockMvcResultMatchers.jsonPath(COMMAND_ARGS_PATH, Matchers.is(commandArgs)))
            .andExpect(MockMvcResultMatchers.jsonPath(SETUP_FILE_PATH, Matchers.is(setupFile)))
            .andExpect(MockMvcResultMatchers.jsonPath(CLUSTER_CRITERIAS_PATH, Matchers.hasSize(1)))
            .andExpect(MockMvcResultMatchers.jsonPath(CLUSTER_CRITERIAS_PATH + "[0].tags", Matchers.hasSize(1)))
            .andExpect(MockMvcResultMatchers.jsonPath(CLUSTER_CRITERIAS_PATH + "[0].tags[0]", Matchers.is(clusterTag)))
            .andExpect(MockMvcResultMatchers.jsonPath(COMMAND_CRITERIA_PATH, Matchers.hasSize(1)))
            .andExpect(MockMvcResultMatchers.jsonPath(COMMAND_CRITERIA_PATH + "[0]", Matchers.is(commandTag)))
            .andExpect(MockMvcResultMatchers.jsonPath(GROUP_PATH, Matchers.nullValue()))
            .andExpect(MockMvcResultMatchers.jsonPath(DISABLE_LOG_ARCHIVAL_PATH, Matchers.is(true)))
            .andExpect(MockMvcResultMatchers.jsonPath(DEPENDENCIES_PATH, Matchers.hasSize(1)))
            .andExpect(MockMvcResultMatchers.jsonPath(DEPENDENCIES_PATH + "[0]", Matchers.is(depFile1)))
            .andExpect(MockMvcResultMatchers.jsonPath(EMAIL_PATH, Matchers.nullValue()))
            .andExpect(MockMvcResultMatchers.jsonPath(CPU_PATH, Matchers.nullValue()))
            .andExpect(MockMvcResultMatchers.jsonPath(MEMORY_PATH, Matchers.nullValue()))
            .andExpect(MockMvcResultMatchers.jsonPath(APPLICATIONS_PATH, Matchers.empty()))
            .andDo(getResultHandler);
    }

    private void checkJobExecution(final int documentationId, final String id) throws Exception {
        final RestDocumentationResultHandler getResultHandler = MockMvcRestDocumentation.document(
            "{class-name}/" + documentationId + "/getJobExecution/",
            Preprocessors.preprocessRequest(Preprocessors.prettyPrint()),
            Preprocessors.preprocessResponse(Preprocessors.prettyPrint()),
            Snippets.ID_PATH_PARAM, // Path parameters
            Snippets.HAL_CONTENT_TYPE_HEADER, // Response Headers
            Snippets.getJobExecutionResponsePayload(), // Response fields
            Snippets.JOB_EXECUTION_LINKS // Links
        );
        this.mvc
            .perform(RestDocumentationRequestBuilders.get(JOBS_API + "/{id}/execution", id))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.jsonPath(ID_PATH, Matchers.is(id)))
            .andExpect(MockMvcResultMatchers.jsonPath(CREATED_PATH, Matchers.notNullValue()))
            .andExpect(MockMvcResultMatchers.jsonPath(UPDATED_PATH, Matchers.notNullValue()))
            .andExpect(MockMvcResultMatchers.jsonPath(HOST_NAME_PATH, Matchers.is(this.hostname)))
            .andExpect(MockMvcResultMatchers.jsonPath(PROCESS_ID_PATH, Matchers.notNullValue()))
            .andExpect(MockMvcResultMatchers.jsonPath(CHECK_DELAY_PATH, Matchers.is((int) CHECK_DELAY)))
            .andExpect(MockMvcResultMatchers.jsonPath(EXIT_CODE_PATH, Matchers.is(JobExecution.SUCCESS_EXIT_CODE)))
            .andDo(getResultHandler);
    }

    private void checkJobCluster(final int documentationId, final String id) throws Exception {
        final RestDocumentationResultHandler getResultHandler = MockMvcRestDocumentation.document(
            "{class-name}/" + documentationId + "/getJobCluster/",
            Preprocessors.preprocessRequest(Preprocessors.prettyPrint()),
            Preprocessors.preprocessResponse(Preprocessors.prettyPrint()),
            Snippets.ID_PATH_PARAM, // Path parameters
            Snippets.HAL_CONTENT_TYPE_HEADER, // Response Headers
            Snippets.getClusterResponsePayload(), // Response fields
            Snippets.CLUSTER_LINKS // Links
        );
        this.mvc
            .perform(RestDocumentationRequestBuilders.get(JOBS_API + "/{id}/cluster", id))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.jsonPath(ID_PATH, Matchers.is(CLUSTER1_ID)))
            .andExpect(MockMvcResultMatchers.jsonPath(CREATED_PATH, Matchers.notNullValue()))
            .andExpect(MockMvcResultMatchers.jsonPath(UPDATED_PATH, Matchers.notNullValue()))
            .andExpect(MockMvcResultMatchers.jsonPath(NAME_PATH, Matchers.is(CLUSTER1_NAME)))
            .andExpect(MockMvcResultMatchers.jsonPath(USER_PATH, Matchers.is(CLUSTER1_USER)))
            .andExpect(MockMvcResultMatchers.jsonPath(VERSION_PATH, Matchers.is(CLUSTER1_VERSION)))
            .andDo(getResultHandler);
    }

    private void checkJobCommand(final int documentationId, final String id) throws Exception {
        final RestDocumentationResultHandler getResultHandler = MockMvcRestDocumentation.document(
            "{class-name}/" + documentationId + "/getJobCommand/",
            Preprocessors.preprocessRequest(Preprocessors.prettyPrint()),
            Preprocessors.preprocessResponse(Preprocessors.prettyPrint()),
            Snippets.ID_PATH_PARAM, // Path parameters
            Snippets.HAL_CONTENT_TYPE_HEADER, // Response Headers
            Snippets.getCommandResponsePayload(), // Response fields
            Snippets.COMMAND_LINKS // Links
        );
        this.mvc
            .perform(RestDocumentationRequestBuilders.get(JOBS_API + "/{id}/command", id))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.jsonPath(ID_PATH, Matchers.is(CMD1_ID)))
            .andExpect(MockMvcResultMatchers.jsonPath(CREATED_PATH, Matchers.notNullValue()))
            .andExpect(MockMvcResultMatchers.jsonPath(UPDATED_PATH, Matchers.notNullValue()))
            .andExpect(MockMvcResultMatchers.jsonPath(NAME_PATH, Matchers.is(CMD1_NAME)))
            .andExpect(MockMvcResultMatchers.jsonPath(USER_PATH, Matchers.is(CMD1_USER)))
            .andExpect(MockMvcResultMatchers.jsonPath(VERSION_PATH, Matchers.is(CMD1_VERSION)))
            .andExpect(MockMvcResultMatchers.jsonPath("$.executable", Matchers.is(CMD1_EXECUTABLE)))
            .andDo(getResultHandler);
    }

    private void checkJobApplications(final int documentationId, final String id) throws Exception {
        final RestDocumentationResultHandler getResultHandler = MockMvcRestDocumentation.document(
            "{class-name}/" + documentationId + "/getJobApplications/",
            Preprocessors.preprocessRequest(Preprocessors.prettyPrint()),
            Preprocessors.preprocessResponse(Preprocessors.prettyPrint()),
            Snippets.ID_PATH_PARAM, // Path parameters
            Snippets.HAL_CONTENT_TYPE_HEADER, // Response Headers
            PayloadDocumentation.responseFields(
                PayloadDocumentation
                    .fieldWithPath("[]")
                    .description("The applications for the job")
                    .attributes(Snippets.EMPTY_CONSTRAINTS)
            ) // Response fields
        );
        this.mvc
            .perform(RestDocumentationRequestBuilders.get(JOBS_API + "/{id}/applications", id))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.jsonPath("$", Matchers.hasSize(2)))
            .andExpect(MockMvcResultMatchers.jsonPath("$[0].id", Matchers.is(APP1_ID)))
            .andExpect(MockMvcResultMatchers.jsonPath("$[1].id", Matchers.is(APP2_ID)))
            .andDo(getResultHandler);
    }

    private void checkFindJobs(final int documentationId, final String id, final String user) throws Exception {
        final RestDocumentationResultHandler findResultHandler = MockMvcRestDocumentation.document(
            "{class-name}/" + documentationId + "/findJobs/",
            Preprocessors.preprocessRequest(Preprocessors.prettyPrint()),
            Preprocessors.preprocessResponse(Preprocessors.prettyPrint()),
            Snippets.JOB_SEARCH_QUERY_PARAMETERS, // Request query parameters
            Snippets.HAL_CONTENT_TYPE_HEADER, // Response headers
            Snippets.JOB_SEARCH_RESULT_FIELDS, // Result fields
            Snippets.SEARCH_LINKS // HAL Links
        );
        this.mvc
            .perform(MockMvcRequestBuilders.get(JOBS_API).param("user", user))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentType(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.jsonPath(JOBS_LIST_PATH, Matchers.hasSize(1)))
            .andExpect(MockMvcResultMatchers.jsonPath(JOBS_LIST_PATH + "[0].id", Matchers.is(id)))
            .andDo(findResultHandler);
    }

    private void testForConflicts(
        final String id,
        final String commandArgs,
        final List<ClusterCriteria> clusterCriteriaList,
        final Set<String> commandCriteria
    ) throws Exception {
        final JobRequest jobConflictRequest = new JobRequest.Builder(
            JOB_NAME,
            JOB_USER,
            JOB_VERSION,
            commandArgs,
            clusterCriteriaList,
            commandCriteria
        )
            .withId(id)
            .withDisableLogArchival(true)
            .build();

        this.mvc
            .perform(
                MockMvcRequestBuilders
                    .post(JOBS_API)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(this.objectMapper.writeValueAsBytes(jobConflictRequest))
            )
            .andExpect(MockMvcResultMatchers.status().isConflict());
    }

    /**
     * Test the job submit method for success twice to validate the file cache use.
     *
     * @throws Exception If there is a problem.
     */
    @Test
    public void testSubmitJobMethodTwiceSuccess() throws Exception {
        submitAndCheckJob(2);
        cleanup();
        setup();
        submitAndCheckJob(3);
    }

    /**
     * Test to make sure we can submit a job with attachments.
     *
     * @throws Exception on any error
     */
    @Test
    public void canSubmitJobWithAttachments() throws Exception {
        final String commandArgs = "-c 'echo hello world'";

        final String clusterTag = "localhost";
        final List<ClusterCriteria> clusterCriteriaList = Lists.newArrayList(
            new ClusterCriteria(Sets.newHashSet(clusterTag))
        );

        final String setUpFile = this.resourceLoader
            .getResource(BASE_DIR + "job" + FILE_DELIMITER + "jobsetupfile")
            .getFile()
            .getAbsolutePath();

        final File attachment1File = this.resourceLoader
            .getResource(BASE_DIR + "job/query.sql")
            .getFile();

        final MockMultipartFile attachment1;
        try (final InputStream is = new FileInputStream(attachment1File)) {
            attachment1 = new MockMultipartFile(
                "attachment",
                attachment1File.getName(),
                MediaType.APPLICATION_OCTET_STREAM_VALUE,
                is
            );
        }

        final File attachment2File = this.resourceLoader
            .getResource(BASE_DIR + "job/query2.sql")
            .getFile();

        final MockMultipartFile attachment2;
        try (final InputStream is = new FileInputStream(attachment2File)) {
            attachment2 = new MockMultipartFile(
                "attachment",
                attachment2File.getName(),
                MediaType.APPLICATION_OCTET_STREAM_VALUE,
                is
            );
        }
        final Set<String> commandCriteria = Sets.newHashSet("bash");
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
            .withDescription(JOB_DESCRIPTION)
            .build();

        this.waitForDone(this.submitJob(4, jobRequest, Lists.newArrayList(attachment1, attachment2)));
    }

    /**
     * Test the job submit method for incorrect cluster resolved.
     *
     * @throws Exception If there is a problem.
     */
    @Test
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
                    .content(this.objectMapper.writeValueAsBytes(jobRequest))
                    .accept(MediaType.APPLICATION_JSON)
            )
            .andExpect(MockMvcResultMatchers.status().isPreconditionFailed());

        Assert.assertThat(this.getStatus(jobId), Matchers.is("{\"status\":\"FAILED\"}"));
    }

    /**
     * Test the job submit method for incorrect cluster criteria.
     *
     * @throws Exception If there is a problem.
     */
    @Test
    public void testSubmitJobMethodInvalidClusterCriteria() throws Exception {
        Assume.assumeTrue(SystemUtils.IS_OS_UNIX);
        final String commandArgs = "-c 'echo hello world'";

        final List<ClusterCriteria> clusterCriteriaList
            = Lists.newArrayList(new ClusterCriteria(Sets.newHashSet(" ", "", null)));

        final String jobId = UUID.randomUUID().toString();

        final Set<String> commandCriteria = Sets.newHashSet("bash");
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
                    .content(this.objectMapper.writeValueAsBytes(jobRequest))
                    .accept(MediaType.APPLICATION_JSON)
            )
            .andExpect(MockMvcResultMatchers.status().isPreconditionFailed());

        this.mvc
            .perform(MockMvcRequestBuilders.get(JOBS_API + "/{id}/status", jobId))
            .andExpect(MockMvcResultMatchers.status().isNotFound());
    }

    /**
     * Test the job submit method for incorrect cluster criteria.
     *
     * @throws Exception If there is a problem.
     */
    @Test
    public void testSubmitJobMethodInvalidCommandCriteria() throws Exception {
        Assume.assumeTrue(SystemUtils.IS_OS_UNIX);
        final String commandArgs = "-c 'echo hello world'";

        final List<ClusterCriteria> clusterCriteriaList
            = Lists.newArrayList(new ClusterCriteria(Sets.newHashSet("ok")));

        final String jobId = UUID.randomUUID().toString();

        final Set<String> commandCriteria = Sets.newHashSet(" ", "", null);
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
                    .content(this.objectMapper.writeValueAsBytes(jobRequest))
                    .accept(MediaType.APPLICATION_JSON)
            ).andExpect(MockMvcResultMatchers.status().isPreconditionFailed());

        this.mvc
            .perform(MockMvcRequestBuilders.get(JOBS_API + "/{id}/status", jobId))
            .andExpect(MockMvcResultMatchers.status().isNotFound());
    }

    /**
     * Test the job submit method for incorrect command resolved.
     *
     * @throws Exception If there is a problem.
     */
    @Test
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
                    .content(objectMapper.writeValueAsBytes(jobRequest))
            )
            .andExpect(MockMvcResultMatchers.status().isPreconditionFailed());

        Assert.assertThat(this.getStatus(jobId), Matchers.is("{\"status\":\"FAILED\"}"));
    }

    /**
     * Test the job submit method for when the job is killed by sending a DELETE HTTP call.
     *
     * @throws Exception If there is a problem.
     */
    @Test
    public void testSubmitJobMethodKill() throws Exception {
        Assume.assumeTrue(SystemUtils.IS_OS_UNIX);
        final String commandArgs = "-c 'sleep 60'";

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
                    .content(this.objectMapper.writeValueAsBytes(jobRequest))
            )
            .andExpect(MockMvcResultMatchers.status().isAccepted())
            .andExpect(MockMvcResultMatchers.header().string(HttpHeaders.LOCATION, Matchers.notNullValue()))
            .andReturn();

        final String jobId = this.getIdFromLocation(result.getResponse().getHeader(HttpHeaders.LOCATION));
        this.waitForRunning(jobId);

        // Let it run for a couple of seconds
        Thread.sleep(2000);

        // Send a kill request to the job.
        final RestDocumentationResultHandler killResultHandler = MockMvcRestDocumentation.document(
            "{class-name}/killJob/",
            Preprocessors.preprocessRequest(Preprocessors.prettyPrint()),
            Preprocessors.preprocessResponse(Preprocessors.prettyPrint()),
            Snippets.ID_PATH_PARAM
        );
        this.mvc
            .perform(RestDocumentationRequestBuilders.delete(JOBS_API + "/{id}", jobId))
            .andExpect(MockMvcResultMatchers.status().isAccepted())
            .andDo(killResultHandler);

        this.waitForDone(jobId);

        this.mvc
            .perform(MockMvcRequestBuilders.get(JOBS_API + "/" + jobId))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentType(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.jsonPath(ID_PATH, Matchers.is(jobId)))
            .andExpect(MockMvcResultMatchers.jsonPath(STATUS_PATH, Matchers.is(JobStatus.KILLED.toString())))
            .andExpect(MockMvcResultMatchers.jsonPath(
                STATUS_MESSAGE_PATH, Matchers.is(JobStatusMessages.JOB_KILLED_BY_USER)
            ));

        // Kill the job again to make sure it doesn't cause a problem.
        this.mvc
            .perform(MockMvcRequestBuilders.delete(JOBS_API + "/" + jobId))
            .andExpect(MockMvcResultMatchers.status().isAccepted());
    }

    /**
     * Test the job submit method for when the job is killed as it times out.
     *
     * @throws Exception If there is a problem.
     */
    @Test
    public void testSubmitJobMethodKillOnTimeout() throws Exception {
        Assume.assumeTrue(SystemUtils.IS_OS_UNIX);
        final String commandArgs = "-c 'sleep 60'";

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
            .withTimeout(5)
            .withDisableLogArchival(true)
            .build();

        final MvcResult result = this.mvc
            .perform(
                MockMvcRequestBuilders
                    .post(JOBS_API)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(objectMapper.writeValueAsBytes(jobRequest))
            ).andReturn();

        if (result.getResponse().getStatus() != HttpStatus.ACCEPTED.value()) {
            log.error(
                "RESPONSE WASN'T 202 IT WAS: {} AND THE ERROR MESSAGE IS: {} AND THE CONTENT IS {}",
                result.getResponse().getStatus(),
                result.getResponse().getErrorMessage(),
                result.getResponse().getContentAsString()
            );
            Assert.fail();
        }

        final String id = this.getIdFromLocation(result.getResponse().getHeader(HttpHeaders.LOCATION));

        this.waitForDone(id);

        this.mvc
            .perform(MockMvcRequestBuilders.get(JOBS_API + "/{id}", id))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentType(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.jsonPath(ID_PATH, Matchers.is(id)))
            .andExpect(MockMvcResultMatchers.jsonPath(STATUS_PATH, Matchers.is(JobStatus.KILLED.toString())))
            .andExpect(MockMvcResultMatchers.jsonPath(STATUS_MESSAGE_PATH, Matchers.is("Job exceeded timeout.")));
    }

    /**
     * Test the job submit method for when the job fails.
     *
     * @throws Exception If there is a problem.
     */
    @Test
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
                    .content(this.objectMapper.writeValueAsBytes(jobRequest))
            )
            .andExpect(MockMvcResultMatchers.status().isAccepted())
            .andExpect(MockMvcResultMatchers.header().string(HttpHeaders.LOCATION, Matchers.notNullValue()))
            .andReturn();

        final String id = this.getIdFromLocation(result.getResponse().getHeader(HttpHeaders.LOCATION));
        this.waitForDone(id);
        Assert.assertEquals(this.getStatus(id), "{\"status\":\"FAILED\"}");

        this.mvc
            .perform(MockMvcRequestBuilders.get(JOBS_API + "/{id}", id))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andExpect(MockMvcResultMatchers.content().contentType(MediaTypes.HAL_JSON))
            .andExpect(MockMvcResultMatchers.jsonPath(ID_PATH, Matchers.is(id)))
            .andExpect(MockMvcResultMatchers.jsonPath(STATUS_PATH, Matchers.is(JobStatus.FAILED.toString())))
            .andExpect(MockMvcResultMatchers.jsonPath(STATUS_MESSAGE_PATH, Matchers.is(JobStatusMessages.JOB_FAILED)));
    }

    private String getIdFromLocation(final String location) {
        return location.substring(location.lastIndexOf("/") + 1);
    }

    private String getExpectedRunContents(
        final String runFileContents,
        final String jobWorkingDir,
        final String jobId
    ) {
        return runFileContents
            .replace("TEST_GENIE_JOB_WORKING_DIR_PLACEHOLDER", jobWorkingDir)
            .replace("JOB_ID_PLACEHOLDER", jobId)
            .replace("COMMAND_ID_PLACEHOLDER", CMD1_ID)
            .replace("COMMAND_NAME_PLACEHOLDER", CMD1_NAME)
            .replace("CLUSTER_ID_PLACEHOLDER", CLUSTER1_ID)
            .replace("CLUSTER_NAME_PLACEHOLDER", CLUSTER1_NAME);
    }

    private String getStatus(final String jobId) throws Exception {
        return this.mvc
            .perform(MockMvcRequestBuilders.get(JOBS_API + "/{id}/status", jobId))
            .andExpect(MockMvcResultMatchers.status().isOk())
            .andReturn()
            .getResponse()
            .getContentAsString();
    }

    private void waitForDone(final String jobId) throws Exception {
        int counter = 0;
        while (true) {
            final String statusString = this.getStatus(jobId);
            if (statusString.contains("INIT") || statusString.contains("RUNNING")) {
                log.info("Iteration {} sleeping for {} ms", counter, SLEEP_TIME);
                Thread.sleep(SLEEP_TIME);
                counter++;
            } else {
                break;
            }
        }
    }

    private void waitForRunning(final String jobId) throws Exception {
        int counter = 0;
        while (true) {
            final String statusString = this.getStatus(jobId);
            if (statusString.contains("INIT")) {
                log.info("Iteration {} sleeping for {} ms", counter, SLEEP_TIME);
                Thread.sleep(SLEEP_TIME);
                counter++;
            } else {
                break;
            }
        }
    }
}
