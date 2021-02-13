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
package com.netflix.genie.web.apis.rest.v3.controllers;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.Application;
import com.netflix.genie.common.dto.ApplicationStatus;
import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.dto.ClusterCriteria;
import com.netflix.genie.common.dto.ClusterStatus;
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.dto.CommandStatus;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.dto.JobStatusMessages;
import com.netflix.genie.common.external.dtos.v4.ArchiveStatus;
import com.netflix.genie.common.external.dtos.v4.Criterion;
import com.netflix.genie.common.external.util.GenieObjectMapper;
import com.netflix.genie.web.introspection.GenieWebHostInfo;
import com.netflix.genie.web.properties.JobsLocationsProperties;
import com.netflix.genie.web.properties.LocalAgentLauncherProperties;
import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import io.restassured.response.ValidatableResponse;
import io.restassured.specification.RequestSpecification;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.SystemUtils;
import org.assertj.core.api.Assertions;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.hateoas.MediaTypes;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.restdocs.headers.HeaderDocumentation;
import org.springframework.restdocs.payload.PayloadDocumentation;
import org.springframework.restdocs.request.RequestDocumentation;
import org.springframework.restdocs.restassured3.RestAssuredRestDocumentation;
import org.springframework.restdocs.restassured3.RestDocumentationFilter;
import org.springframework.test.context.TestPropertySource;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
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
@TestPropertySource(
    properties = {
        LocalAgentLauncherProperties.PROPERTY_PREFIX + ".run-as-user=false",
    }
)
class JobRestControllerIntegrationTest extends RestControllerIntegrationTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(JobRestControllerIntegrationTest.class);

    private static final long SLEEP_TIME = 500L;
    private static final String SCHEDULER_JOB_NAME_KEY = "schedulerJobName";
    private static final String SCHEDULER_RUN_ID_KEY = "schedulerRunId";
    private static final String COMMAND_ARGS_PATH = "commandArgs";
    private static final String STATUS_MESSAGE_PATH = "statusMsg";
    private static final String CLUSTER_NAME_PATH = "clusterName";
    private static final String COMMAND_NAME_PATH = "commandName";
    private static final String ARCHIVE_LOCATION_PATH = "archiveLocation";
    private static final String STARTED_PATH = "started";
    private static final String FINISHED_PATH = "finished";
    private static final String CLUSTER_CRITERIAS_PATH = "clusterCriterias";
    private static final String COMMAND_CRITERIA_PATH = "commandCriteria";
    private static final String GROUP_PATH = "group";
    private static final String DISABLE_LOG_ARCHIVAL_PATH = "disableLogArchival";
    private static final String EMAIL_PATH = "email";
    private static final String CPU_PATH = "cpu";
    private static final String MEMORY_PATH = "memory";
    private static final String APPLICATIONS_PATH = "applications";
    private static final String HOST_NAME_PATH = "hostName";
    private static final String PROCESS_ID_PATH = "processId";
    private static final String CHECK_DELAY_PATH = "checkDelay";
    private static final String EXIT_CODE_PATH = "exitCode";
    private static final String CLIENT_HOST_PATH = "clientHost";
    private static final String USER_AGENT_PATH = "userAgent";
    private static final String NUM_ATTACHMENTS_PATH = "numAttachments";
    private static final String TOTAL_SIZE_ATTACHMENTS_PATH = "totalSizeOfAttachments";
    private static final String STD_OUT_SIZE_PATH = "stdOutSize";
    private static final String STD_ERR_SIZE_PATH = "stdErrSize";
    private static final String JOBS_LIST_PATH = EMBEDDED_PATH + ".jobSearchResultList";
    private static final String GROUPING_PATH = "grouping";
    private static final String GROUPING_INSTANCE_PATH = "groupingInstance";
    private static final String ARCHIVE_STATUS_PATH = "archiveStatus";
    private static final String JOB_COMMAND_LINK_PATH = "_links.command.href";
    private static final String JOB_CLUSTER_LINK_PATH = "_links.cluster.href";
    private static final String JOB_APPLICATIONS_LINK_PATH = "_links.applications.href";
    private static final long CHECK_DELAY = 250L;
    private static final String BASE_DIR
        = "com/netflix/genie/web/apis/rest/v3/controllers/JobRestControllerIntegrationTests/";
    private static final String FILE_DELIMITER = "/";
    private static final String LOCALHOST_CLUSTER_TAG = "localhost";
    private static final String BASH_COMMAND_TAG = "bash";
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
    private static final ArrayList<String> CMD1_EXECUTABLE_AND_ARGS = Lists.newArrayList(CMD1_EXECUTABLE);
    private static final String CLUSTER1_ID = "cluster1";
    private static final String CLUSTER1_NAME = "Local laptop";
    private static final String CLUSTER1_USER = "genie";
    private static final String CLUSTER1_VERSION = "1.0";
    private static final String JOB_TAG_1 = "aTag";
    private static final String JOB_TAG_2 = "zTag";
    private static final Set<String> JOB_TAGS = Sets.newHashSet(JOB_TAG_1, JOB_TAG_2);
    private static final String JOB_GROUPING = "Workflow.Foo";
    private static final String JOB_GROUPING_INSTANCE = "Workflow.Foo_Step.Blah-2020021919:12:34.000_1";
    // This file is not UTF-8 encoded. It is uploaded to test server behavior
    // related to charset headers
    private static final String GB18030_TXT = "GB18030.txt";
    private static final List<String> SLEEP_AND_ECHO_COMMAND_ARGS =
        Lists.newArrayList("-c", "'sleep 5 && echo hello world'");
    private static final ArrayList<String> SLEEP_60_COMMAND_ARGS = Lists.newArrayList("-c", "'sleep 60'");
    private static final String EXPECTED_STDOUT_CONTENT = "hello world\n";
    private static final int EXPECTED_STDOUT_LENGTH = EXPECTED_STDOUT_CONTENT.length();

    private ResourceLoader resourceLoader;
    private JsonNode metadata;
    private String schedulerJobName;
    private String schedulerRunId;

    @Autowired
    private JobsLocationsProperties jobsLocationsProperties;

    @Autowired
    private GenieWebHostInfo genieHostInfo;

    @Autowired
    private Resource jobDirResource;

    @BeforeEach
    void beforeJobs() throws Exception {
        this.schedulerJobName = UUID.randomUUID().toString();
        this.schedulerRunId = UUID.randomUUID().toString();
        this.metadata = GenieObjectMapper.getMapper().readTree(
            "{\""
                + SCHEDULER_JOB_NAME_KEY
                + "\":\""
                + this.schedulerJobName
                + "\", \""
                + SCHEDULER_RUN_ID_KEY
                + "\":\""
                + this.schedulerRunId
                + "\"}"
        );

        this.resourceLoader = new DefaultResourceLoader();
        this.createAnApplication(APP1_ID, APP1_NAME);
        this.createAnApplication(APP2_ID, APP2_NAME);
        this.createAllClusters();
        this.createAllCommands();
        this.linkAllEntities();
    }

    @Test
    void testSubmitJobMethodSuccess() throws Exception {
        this.submitAndCheckJob(1, true);
    }

    @Test
    void testForTooManyCommandArgs() throws Exception {
        final JobRequest tooManyCommandArguments = new JobRequest.Builder(
            JOB_NAME,
            JOB_USER,
            JOB_VERSION,
            Lists.newArrayList(new ClusterCriteria(Sets.newHashSet(LOCALHOST_CLUSTER_TAG))),
            Sets.newHashSet(BASH_COMMAND_TAG)
        )
            .withCommandArgs(StringUtils.leftPad("bad", 10_001, 'a'))
            .build();

        RestAssured
            .given(this.getRequestSpecification())
            .contentType(MediaType.APPLICATION_JSON_VALUE)
            .body(GenieObjectMapper.getMapper().writeValueAsBytes(tooManyCommandArguments))
            .when()
            .port(this.port)
            .post(JOBS_API)
            .then()
            .contentType(Matchers.startsWith(MediaType.APPLICATION_JSON_VALUE))
            .statusCode(Matchers.is(HttpStatus.PRECONDITION_FAILED.value()))
            .body(
                EXCEPTION_MESSAGE_PATH,
                Matchers.containsString("The maximum number of characters for the command arguments is 10,000"
                )
            );
    }

    private void submitAndCheckJob(final int documentationId, final boolean archiveJob) throws Exception {
        Assumptions.assumeTrue(SystemUtils.IS_OS_UNIX);
        final List<String> commandArgs = SLEEP_AND_ECHO_COMMAND_ARGS;

        final String clusterTag = LOCALHOST_CLUSTER_TAG;
        final List<ClusterCriteria> clusterCriteriaList = Lists.newArrayList(
            new ClusterCriteria(Sets.newHashSet(clusterTag))
        );

        final String setUpFile = this.getResourceURI(BASE_DIR + "job" + FILE_DELIMITER + "jobsetupfile");

        final String configFile1 = this.getResourceURI(BASE_DIR + "job" + FILE_DELIMITER + "config1");
        final Set<String> configs = Sets.newHashSet(configFile1);

        final String depFile1 = this.getResourceURI(BASE_DIR + "job" + FILE_DELIMITER + "dep1");
        final Set<String> dependencies = Sets.newHashSet(depFile1);

        final String commandTag = BASH_COMMAND_TAG;
        final Set<String> commandCriteria = Sets.newHashSet(commandTag);
        final JobRequest jobRequest = new JobRequest.Builder(
            JOB_NAME,
            JOB_USER,
            JOB_VERSION,
            clusterCriteriaList,
            commandCriteria
        )
            .withCommandArgs(SLEEP_AND_ECHO_COMMAND_ARGS)
            .withDisableLogArchival(!archiveJob)
            .withSetupFile(setUpFile)
            .withConfigs(configs)
            .withDependencies(dependencies)
            .withDescription(JOB_DESCRIPTION)
            .withMetadata(this.metadata)
            .withTags(JOB_TAGS)
            .withGrouping(JOB_GROUPING)
            .withGroupingInstance(JOB_GROUPING_INSTANCE)
            .build();

        final String id = this.submitJob(documentationId, jobRequest, null);
        this.waitForDone(id);

        this.checkJobStatus(documentationId, id);
        this.checkJob(documentationId, id, commandArgs);
        if (archiveJob) {
            this.checkJobOutput(documentationId, id);
        }
        this.checkJobRequest(
            documentationId,
            id,
            commandArgs,
            setUpFile,
            clusterTag,
            commandTag,
            configFile1,
            depFile1,
            archiveJob
        );
        this.checkJobExecution(documentationId, id);
        this.checkJobMetadata(documentationId, id);
        this.checkJobCluster(documentationId, id);
        this.checkJobCommand(documentationId, id);
        this.checkJobApplications(documentationId, id);
        this.checkFindJobs(documentationId, id, JOB_USER);
        this.checkJobArchive(id);

        Assertions.assertThat(this.jobRepository.count()).isEqualTo(1L);

        // Test for conflicts
        this.testForConflicts(id, commandArgs, clusterCriteriaList, commandCriteria);
    }

    private String submitJob(
        final int documentationId,
        final JobRequest jobRequest,
        @Nullable final List<MockMultipartFile> attachments
    ) throws Exception {
        if (attachments != null) {
            final RestDocumentationFilter createResultFilter = RestAssuredRestDocumentation.document(
                "{class-name}/" + documentationId + "/submitJobWithAttachments/",
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

            final RequestSpecification jobRequestSpecification = RestAssured
                .given(this.getRequestSpecification())
                .filter(createResultFilter)
                .contentType(MediaType.MULTIPART_FORM_DATA_VALUE)
                .multiPart(
                    "request",
                    GenieObjectMapper.getMapper().writeValueAsString(jobRequest),
                    MediaType.APPLICATION_JSON_VALUE
                );

            for (final MockMultipartFile attachment : attachments) {
                jobRequestSpecification.multiPart(
                    "attachment",
                    attachment.getOriginalFilename(),
                    attachment.getBytes(),
                    MediaType.APPLICATION_OCTET_STREAM_VALUE
                );
            }

            return this.getIdFromLocation(
                jobRequestSpecification
                    .when()
                    .port(this.port)
                    .post(JOBS_API)
                    .then()
                    .statusCode(Matchers.is(HttpStatus.ACCEPTED.value()))
                    .header(HttpHeaders.LOCATION, Matchers.notNullValue())
                    .extract()
                    .header(HttpHeaders.LOCATION)
            );
        } else {
            // Use regular POST
            final RestDocumentationFilter createResultFilter = RestAssuredRestDocumentation.document(
                "{class-name}/" + documentationId + "/submitJobWithoutAttachments/",
                Snippets.CONTENT_TYPE_HEADER, // Request headers
                Snippets.getJobRequestRequestPayload(), // Request Fields
                Snippets.LOCATION_HEADER // Response Headers
            );

            return this.getIdFromLocation(
                RestAssured
                    .given(this.getRequestSpecification())
                    .filter(createResultFilter)
                    .contentType(MediaType.APPLICATION_JSON_VALUE)
                    .body(GenieObjectMapper.getMapper().writeValueAsBytes(jobRequest))
                    .when()
                    .port(this.port)
                    .post(JOBS_API)
                    .then()
                    .statusCode(Matchers.is(HttpStatus.ACCEPTED.value()))
                    .header(HttpHeaders.LOCATION, Matchers.notNullValue())
                    .extract()
                    .header(HttpHeaders.LOCATION)
            );
        }
    }

    private void checkJobStatus(final int documentationId, final String id) {
        final RestDocumentationFilter getResultFilter = RestAssuredRestDocumentation.document(
            "{class-name}/" + documentationId + "/getJobStatus/",
            Snippets.ID_PATH_PARAM, // Path parameters
            Snippets.JSON_CONTENT_TYPE_HEADER, // Response Headers
            PayloadDocumentation.responseFields(
                PayloadDocumentation
                    .fieldWithPath("status")
                    .description("The job status. One of: " + Arrays.toString(JobStatus.values()))
                    .attributes(Snippets.EMPTY_CONSTRAINTS)
            ) // Response fields
        );

        RestAssured
            .given(this.getRequestSpecification())
            .filter(getResultFilter)
            .when()
            .port(this.port)
            .get(JOBS_API + "/{id}/status", id)
            .then()
            .contentType(Matchers.containsString(MediaType.APPLICATION_JSON_VALUE))
            .body(STATUS_PATH, Matchers.is(JobStatus.SUCCEEDED.toString()));
    }

    private void checkJob(
        final int documentationId,
        final String id,
        final List<String> commandArgs
    ) {
        final RestDocumentationFilter getResultFilter = RestAssuredRestDocumentation.document(
            "{class-name}/" + documentationId + "/getJob/",
            Snippets.ID_PATH_PARAM, // Path parameters
            Snippets.HAL_CONTENT_TYPE_HEADER, // Response Headers
            Snippets.getJobResponsePayload(), // Response fields
            Snippets.JOB_LINKS // Links
        );
        RestAssured
            .given(this.getRequestSpecification())
            .filter(getResultFilter)
            .when()
            .port(this.port)
            .get(JOBS_API + "/{id}", id)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.containsString(MediaTypes.HAL_JSON_VALUE))
            .body(ID_PATH, Matchers.is(id))
            .body(CREATED_PATH, Matchers.notNullValue())
            .body(UPDATED_PATH, Matchers.notNullValue())
            .body(VERSION_PATH, Matchers.is(JOB_VERSION))
            .body(USER_PATH, Matchers.is(JOB_USER))
            .body(NAME_PATH, Matchers.is(JOB_NAME))
            .body(DESCRIPTION_PATH, Matchers.is(JOB_DESCRIPTION))
            .body(METADATA_PATH + "." + SCHEDULER_JOB_NAME_KEY, Matchers.is(this.schedulerJobName))
            .body(METADATA_PATH + "." + SCHEDULER_RUN_ID_KEY, Matchers.is(this.schedulerRunId))
            .body(COMMAND_ARGS_PATH, Matchers.is(StringUtils.join(commandArgs, StringUtils.SPACE)))
            .body(STATUS_PATH, Matchers.is(JobStatus.SUCCEEDED.toString()))
            .body(STATUS_MESSAGE_PATH, Matchers.is(JOB_STATUS_MSG))
            .body(STARTED_PATH, Matchers.not(Instant.EPOCH))
            .body(FINISHED_PATH, Matchers.notNullValue())
            // TODO: Flipped during V4 migration to always be on to replicate expected behavior of V3 until clients
            //       can be migrated
//            .body(ARCHIVE_LOCATION_PATH, archiveJob ? Matchers.notNullValue() : Matchers.isEmptyOrNullString())
            .body(ARCHIVE_LOCATION_PATH, Matchers.notNullValue())
            .body(CLUSTER_NAME_PATH, Matchers.is(CLUSTER1_NAME))
            .body(COMMAND_NAME_PATH, Matchers.is(CMD1_NAME))
            .body(TAGS_PATH, Matchers.contains(JOB_TAG_1, JOB_TAG_2))
            .body(GROUPING_PATH, Matchers.is(JOB_GROUPING))
            .body(GROUPING_INSTANCE_PATH, Matchers.is(JOB_GROUPING_INSTANCE))
            .body(LINKS_PATH + ".keySet().size()", Matchers.is(9))
            .body(LINKS_PATH, Matchers.hasKey(SELF_LINK_KEY))
            .body(LINKS_PATH, Matchers.hasKey("request"))
            .body(LINKS_PATH, Matchers.hasKey("execution"))
            .body(LINKS_PATH, Matchers.hasKey("output"))
            .body(LINKS_PATH, Matchers.hasKey("status"))
            .body(LINKS_PATH, Matchers.hasKey("cluster"))
            .body(LINKS_PATH, Matchers.hasKey("command"))
            .body(LINKS_PATH, Matchers.hasKey("applications"))
            .body(LINKS_PATH, Matchers.hasKey("metadata"))
            .body(
                JOB_CLUSTER_LINK_PATH,
                EntityLinkMatcher.matchUri(JOBS_API, "cluster", null, id))
            .body(
                JOB_COMMAND_LINK_PATH,
                EntityLinkMatcher.matchUri(JOBS_API, "command", null, id))
            .body(
                JOB_APPLICATIONS_LINK_PATH,
                EntityLinkMatcher.matchUri(JOBS_API, APPLICATIONS_LINK_KEY, null, id)
            );
    }

    private void checkJobOutput(final int documentationId, final String id) throws Exception {
        // Check getting a directory as json
        final RestDocumentationFilter jsonResultFilter = RestAssuredRestDocumentation.document(
            "{class-name}/" + documentationId + "/getJobOutput/json/",
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

        final ValidatableResponse outputDirJsonResponse = RestAssured
            .given(this.getRequestSpecification())
            .filter(jsonResultFilter)
            .accept(MediaType.APPLICATION_JSON_VALUE)
            .when()
            .port(this.port)
            .get(JOBS_API + "/{id}/output/{filePath}", id, "")
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.startsWith(MediaType.APPLICATION_JSON_VALUE));

        outputDirJsonResponse
            .body("parent", Matchers.blankOrNullString())
            .body("directories[0].name", Matchers.is("genie/"))
            .body("files[0].name", Matchers.is("config1"))
            .body("files[1].name", Matchers.is("dep1"))
            .body("files[2].name", Matchers.is("genie_setup.sh"))
            .body("files[3].name", Matchers.is("run"))
            .body("files[4].name", Matchers.is("stderr"))
            .body("files[5].name", Matchers.is("stdout"));

        // Check getting a directory as HTML
        final RestDocumentationFilter htmlResultFilter = RestAssuredRestDocumentation.document(
            "{class-name}/" + documentationId + "/getJobOutput/html/",
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

        RestAssured
            .given(this.getRequestSpecification())
            .filter(htmlResultFilter)
            .accept(MediaType.TEXT_HTML_VALUE)
            .when()
            .port(this.port)
            .get(JOBS_API + "/{id}/output/{filePath}", id, "")
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.containsString(MediaType.TEXT_HTML_VALUE));

        // Check getting a file
        final RestDocumentationFilter fileResultFilter = RestAssuredRestDocumentation.document(
            "{class-name}/" + documentationId + "/getJobOutput/file/",
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

        final String setupFile = this.getResourceURI(BASE_DIR + "job" + FILE_DELIMITER + "jobsetupfile");
        final String setupFileContents = new String(
            Files.readAllBytes(Paths.get(new URI(setupFile))),
            StandardCharsets.UTF_8
        );

        RestAssured
            .given(this.getRequestSpecification())
            .filter(fileResultFilter)
            .when()
            .port(this.port)
            .get(
                JOBS_API + "/{id}/output/{filePath}",
                id,
                "genie_setup.sh"
            )
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .body(Matchers.is(setupFileContents));

        // Validate content of 'run' file
        final String expectedFilename = "runsh-agent.txt";
        final String runFile = this.getResourceURI(BASE_DIR + expectedFilename);
        final String runFileContent = new String(
            Files.readAllBytes(Paths.get(new URI(runFile))),
            StandardCharsets.UTF_8
        );

        final String testJobsDir = this.jobsLocationsProperties.getJobs().getPath();
        final String expectedRunFileContent = this.getExpectedRunContents(
            runFileContent,
            testJobsDir + id,
            id
        );

        RestAssured
            .given(this.getRequestSpecification())
            .filter(fileResultFilter)
            .when()
            .port(this.port)
            .get(JOBS_API + "/{id}/output/{filePath}", id, "run")
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .body(Matchers.equalTo(expectedRunFileContent));

        // Check stderr content and size
        RestAssured
            .given(this.getRequestSpecification())
            .when()
            .port(this.port)
            .get(JOBS_API + "/{id}/output/{filePath}", id, "stderr")
            .then()
            .header(HttpHeaders.CONTENT_LENGTH, String.valueOf(0))
            .body(Matchers.emptyString());

        // Check stdout content and size
        RestAssured
            .given(this.getRequestSpecification())
            .when()
            .port(this.port)
            .get(JOBS_API + "/{id}/output/{filePath}", id, "stdout")
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .header(HttpHeaders.CONTENT_LENGTH, Integer.toString(EXPECTED_STDOUT_LENGTH))
            .body(Matchers.equalTo(EXPECTED_STDOUT_CONTENT));

        checkGetFileRanges(id);
    }

    private void checkGetFileRanges(final String id) {
        // Range request -- full range
        RestAssured
            .given(this.getRequestSpecification())
            .header(HttpHeaders.RANGE, "bytes=0-" + (EXPECTED_STDOUT_LENGTH - 1))
            .when()
            .port(this.port)
            .get(JOBS_API + "/{id}/output/{filePath}", id, "stdout")
            .then()
            .statusCode(Matchers.is(HttpStatus.PARTIAL_CONTENT.value()))
            .header(HttpHeaders.CONTENT_LENGTH, Integer.toString(EXPECTED_STDOUT_LENGTH))
            .body(Matchers.equalTo(EXPECTED_STDOUT_CONTENT));

        // Range request -- start offset
        RestAssured
            .given(this.getRequestSpecification())
            .header(HttpHeaders.RANGE, "bytes=" + (EXPECTED_STDOUT_LENGTH - 2) + "-")
            .when()
            .port(this.port)
            .get(JOBS_API + "/{id}/output/{filePath}", id, "stdout")
            .then()
            .statusCode(Matchers.is(HttpStatus.PARTIAL_CONTENT.value()))
            .header(HttpHeaders.CONTENT_LENGTH, Integer.toString(2))
            .body(Matchers.equalTo(EXPECTED_STDOUT_CONTENT.substring(EXPECTED_STDOUT_LENGTH - 2)));

        // Range request -- suffix
        RestAssured
            .given(this.getRequestSpecification())
            .header(HttpHeaders.RANGE, "bytes=" + (-2))
            .when()
            .port(this.port)
            .get(JOBS_API + "/{id}/output/{filePath}", id, "stdout")
            .then()
            .statusCode(Matchers.is(HttpStatus.PARTIAL_CONTENT.value()))
            .header(HttpHeaders.CONTENT_LENGTH, Integer.toString(2))
            .body(Matchers.equalTo(EXPECTED_STDOUT_CONTENT.substring(EXPECTED_STDOUT_LENGTH - 2)));

        // Range request -- range
        RestAssured
            .given(this.getRequestSpecification())
            .header(HttpHeaders.RANGE, "bytes=6-10")
            .when()
            .port(this.port)
            .get(JOBS_API + "/{id}/output/{filePath}", id, "stdout")
            .then()
            .statusCode(Matchers.is(HttpStatus.PARTIAL_CONTENT.value()))
            .header(HttpHeaders.CONTENT_LENGTH, Integer.toString(5))
            .body(Matchers.equalTo(EXPECTED_STDOUT_CONTENT.substring(6, 11)));

        // Range request -- request more than it's available
        RestAssured
            .given(this.getRequestSpecification())
            .header(HttpHeaders.RANGE, "bytes=0-1000")
            .when()
            .port(this.port)
            .get(JOBS_API + "/{id}/output/{filePath}", id, "stdout")
            .then()
            .header(HttpHeaders.CONTENT_LENGTH, Integer.toString(EXPECTED_STDOUT_LENGTH))
            .body(Matchers.equalTo(EXPECTED_STDOUT_CONTENT));

        // Range request -- out of range
        RestAssured
            .given(this.getRequestSpecification())
            .header(HttpHeaders.RANGE, "bytes=" + EXPECTED_STDOUT_LENGTH + "-")
            .when()
            .port(this.port)
            .get(JOBS_API + "/{id}/output/{filePath}", id, "stdout")
            .then()
            .statusCode(Matchers.is(HttpStatus.REQUESTED_RANGE_NOT_SATISFIABLE.value()))
            .header(HttpHeaders.CONTENT_LENGTH, Matchers.blankOrNullString());
    }

    private void checkJobRequest(
        final int documentationId,
        final String id,
        final List<String> commandArgs,
        final String setupFile,
        final String clusterTag,
        final String commandTag,
        final String configFile1,
        final String depFile1,
        final boolean archiveJob
    ) {
        final RestDocumentationFilter getResultFilter = RestAssuredRestDocumentation.document(
            "{class-name}/" + documentationId + "/getJobRequest/",
            Snippets.ID_PATH_PARAM, // Path parameters
            Snippets.HAL_CONTENT_TYPE_HEADER, // Response Headers
            Snippets.getJobRequestResponsePayload(), // Response fields
            Snippets.JOB_REQUEST_LINKS // Links
        );

        RestAssured
            .given(this.getRequestSpecification())
            .filter(getResultFilter)
            .when()
            .port(this.port)
            .get(JOBS_API + "/{id}/request", id)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.containsString(MediaTypes.HAL_JSON_VALUE))
            .body(ID_PATH, Matchers.is(id))
            .body(CREATED_PATH, Matchers.notNullValue())
            .body(UPDATED_PATH, Matchers.notNullValue())
            .body(NAME_PATH, Matchers.is(JOB_NAME))
            .body(VERSION_PATH, Matchers.is(JOB_VERSION))
            .body(USER_PATH, Matchers.is(JOB_USER))
            .body(DESCRIPTION_PATH, Matchers.is(JOB_DESCRIPTION))
            .body(METADATA_PATH + "." + SCHEDULER_JOB_NAME_KEY, Matchers.is(this.schedulerJobName))
            .body(METADATA_PATH + "." + SCHEDULER_RUN_ID_KEY, Matchers.is(this.schedulerRunId))
            .body(COMMAND_ARGS_PATH, Matchers.is(StringUtils.join(commandArgs, StringUtils.SPACE)))
            .body(SETUP_FILE_PATH, Matchers.is(setupFile))
            .body(CLUSTER_CRITERIAS_PATH, Matchers.hasSize(1))
            .body(CLUSTER_CRITERIAS_PATH + "[0].tags", Matchers.hasSize(1))
            .body(CLUSTER_CRITERIAS_PATH + "[0].tags[0]", Matchers.is(clusterTag))
            .body(COMMAND_CRITERIA_PATH, Matchers.hasSize(1))
            .body(COMMAND_CRITERIA_PATH + "[0]", Matchers.is(commandTag))
            .body(GROUP_PATH, Matchers.nullValue())
            .body(DISABLE_LOG_ARCHIVAL_PATH, Matchers.is(!archiveJob))
            .body(CONFIGS_PATH, Matchers.hasSize(1))
            .body(CONFIGS_PATH + "[0]", Matchers.is(configFile1))
            .body(DEPENDENCIES_PATH, Matchers.hasSize(1))
            .body(DEPENDENCIES_PATH + "[0]", Matchers.is(depFile1))
            .body(EMAIL_PATH, Matchers.nullValue())
            .body(CPU_PATH, Matchers.nullValue())
            .body(MEMORY_PATH, Matchers.nullValue())
            .body(APPLICATIONS_PATH, Matchers.empty())
            .body(TAGS_PATH, Matchers.contains(JOB_TAG_1, JOB_TAG_2))
            .body(GROUPING_PATH, Matchers.is(JOB_GROUPING))
            .body(GROUPING_INSTANCE_PATH, Matchers.is(JOB_GROUPING_INSTANCE));
    }

    private void checkJobExecution(final int documentationId, final String id) {
        final RestDocumentationFilter getResultFilter = RestAssuredRestDocumentation.document(
            "{class-name}/" + documentationId + "/getJobExecution/",
            Snippets.ID_PATH_PARAM, // Path parameters
            Snippets.HAL_CONTENT_TYPE_HEADER, // Response Headers
            Snippets.getJobExecutionResponsePayload(), // Response fields
            Snippets.JOB_EXECUTION_LINKS // Links
        );

        final ValidatableResponse validatableResponse = RestAssured
            .given(this.getRequestSpecification())
            .filter(getResultFilter)
            .when()
            .port(this.port)
            .get(JOBS_API + "/{id}/execution", id)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.containsString(MediaTypes.HAL_JSON_VALUE))
            .body(ID_PATH, Matchers.is(id))
            .body(CREATED_PATH, Matchers.notNullValue())
            .body(UPDATED_PATH, Matchers.notNullValue())
            .body(HOST_NAME_PATH, Matchers.is(this.genieHostInfo.getHostname()))
            .body(ARCHIVE_STATUS_PATH, Matchers.is(ArchiveStatus.ARCHIVED.toString()));

        // TODO these are not populated
        validatableResponse
            .body(PROCESS_ID_PATH, Matchers.nullValue())
            .body(CHECK_DELAY_PATH, Matchers.nullValue())
            .body(EXIT_CODE_PATH, Matchers.nullValue());
    }

    private void checkJobMetadata(final int documentationId, final String id) {
        final RestDocumentationFilter getResultFilter = RestAssuredRestDocumentation.document(
            "{class-name}/" + documentationId + "/getJobMetadata/",
            Snippets.ID_PATH_PARAM, // Path parameters
            Snippets.HAL_CONTENT_TYPE_HEADER, // Response Headers
            Snippets.getJobMetadataResponsePayload(), // Response fields
            Snippets.JOB_METADATA_LINKS // Links
        );

        final ValidatableResponse validatableResponse = RestAssured
            .given(this.getRequestSpecification())
            .filter(getResultFilter)
            .when()
            .port(this.port)
            .get(JOBS_API + "/{id}/metadata", id)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.containsString(MediaTypes.HAL_JSON_VALUE))
            .body(ID_PATH, Matchers.is(id))
            .body(CREATED_PATH, Matchers.notNullValue())
            .body(UPDATED_PATH, Matchers.notNullValue())
            .body(CLIENT_HOST_PATH, Matchers.notNullValue())
            .body(USER_AGENT_PATH, Matchers.notNullValue())
            .body(NUM_ATTACHMENTS_PATH, Matchers.notNullValue())
            .body(TOTAL_SIZE_ATTACHMENTS_PATH, Matchers.notNullValue());

        // TODO: These are not populated
        validatableResponse
            .body(STD_OUT_SIZE_PATH, Matchers.nullValue())
            .body(STD_ERR_SIZE_PATH, Matchers.nullValue());
    }

    private void checkJobCluster(final int documentationId, final String id) {
        final RestDocumentationFilter getResultFilter = RestAssuredRestDocumentation.document(
            "{class-name}/" + documentationId + "/getJobCluster/",
            Snippets.ID_PATH_PARAM, // Path parameters
            Snippets.HAL_CONTENT_TYPE_HEADER, // Response Headers
            Snippets.getClusterResponsePayload(), // Response fields
            Snippets.CLUSTER_LINKS // Links
        );

        RestAssured
            .given(this.getRequestSpecification())
            .filter(getResultFilter)
            .when()
            .port(this.port)
            .get(JOBS_API + "/{id}/cluster", id)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.containsString(MediaTypes.HAL_JSON_VALUE))
            .body(ID_PATH, Matchers.is(CLUSTER1_ID))
            .body(CREATED_PATH, Matchers.notNullValue())
            .body(UPDATED_PATH, Matchers.notNullValue())
            .body(NAME_PATH, Matchers.is(CLUSTER1_NAME))
            .body(USER_PATH, Matchers.is(CLUSTER1_USER))
            .body(VERSION_PATH, Matchers.is(CLUSTER1_VERSION));
    }

    private void checkJobCommand(final int documentationId, final String id) {
        final RestDocumentationFilter getResultFilter = RestAssuredRestDocumentation.document(
            "{class-name}/" + documentationId + "/getJobCommand/",
            Snippets.ID_PATH_PARAM, // Path parameters
            Snippets.HAL_CONTENT_TYPE_HEADER, // Response Headers
            Snippets.getCommandResponsePayload(), // Response fields
            Snippets.COMMAND_LINKS // Links
        );

        RestAssured
            .given(this.getRequestSpecification())
            .filter(getResultFilter)
            .when()
            .port(this.port)
            .get(JOBS_API + "/{id}/command", id)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.containsString(MediaTypes.HAL_JSON_VALUE))
            .body(ID_PATH, Matchers.is(CMD1_ID))
            .body(CREATED_PATH, Matchers.notNullValue())
            .body(UPDATED_PATH, Matchers.notNullValue())
            .body(NAME_PATH, Matchers.is(CMD1_NAME))
            .body(USER_PATH, Matchers.is(CMD1_USER))
            .body(VERSION_PATH, Matchers.is(CMD1_VERSION))
            .body("executable", Matchers.is(CMD1_EXECUTABLE))
            .body("executableAndArguments", Matchers.equalTo(CMD1_EXECUTABLE_AND_ARGS));
    }

    private void checkJobApplications(final int documentationId, final String id) {
        final RestDocumentationFilter getResultFilter = RestAssuredRestDocumentation.document(
            "{class-name}/" + documentationId + "/getJobApplications/",
            Snippets.ID_PATH_PARAM, // Path parameters
            Snippets.HAL_CONTENT_TYPE_HEADER, // Response Headers
            PayloadDocumentation.responseFields(
                PayloadDocumentation
                    .subsectionWithPath("[]")
                    .description("The applications for the job")
                    .attributes(Snippets.EMPTY_CONSTRAINTS)
            ) // Response fields
        );

        RestAssured
            .given(this.getRequestSpecification())
            .filter(getResultFilter)
            .when()
            .port(this.port)
            .get(JOBS_API + "/{id}/applications", id)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.containsString(MediaTypes.HAL_JSON_VALUE))
            .body("$", Matchers.hasSize(2))
            .body("[0].id", Matchers.is(APP1_ID))
            .body("[1].id", Matchers.is(APP2_ID));
    }

    private void checkFindJobs(final int documentationId, final String id, final String user) {
        final RestDocumentationFilter findResultFilter = RestAssuredRestDocumentation.document(
            "{class-name}/" + documentationId + "/findJobs/",
            Snippets.JOB_SEARCH_QUERY_PARAMETERS, // Request query parameters
            Snippets.HAL_CONTENT_TYPE_HEADER, // Response headers
            Snippets.JOB_SEARCH_RESULT_FIELDS, // Result fields
            Snippets.SEARCH_LINKS // HAL Links
        );

        RestAssured
            .given(this.getRequestSpecification())
            .filter(findResultFilter)
            .param("user", user)
            .when()
            .port(this.port)
            .get(JOBS_API)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.containsString(MediaTypes.HAL_JSON_VALUE))
            .body(JOBS_LIST_PATH, Matchers.hasSize(1))
            .body(JOBS_LIST_PATH + "[0].id", Matchers.is(id));
    }

    private void testForConflicts(
        final String id,
        final List<String> commandArgs,
        final List<ClusterCriteria> clusterCriteriaList,
        final Set<String> commandCriteria
    ) throws Exception {
        final JobRequest jobConflictRequest = new JobRequest.Builder(
            JOB_NAME,
            JOB_USER,
            JOB_VERSION,
            clusterCriteriaList,
            commandCriteria
        )
            .withId(id)
            .withCommandArgs(commandArgs)
            .withDisableLogArchival(true)
            .build();

        RestAssured
            .given(this.getRequestSpecification())
            .contentType(MediaType.APPLICATION_JSON_VALUE)
            .body(GenieObjectMapper.getMapper().writeValueAsBytes(jobConflictRequest))
            .when()
            .port(this.port)
            .post(JOBS_API)
            .then()
            .statusCode(Matchers.is(HttpStatus.CONFLICT.value()))
            .contentType(Matchers.containsString(MediaType.APPLICATION_JSON_VALUE));
    }

    private void checkJobArchive(
        final String id
    ) {
        final Path archiveDirectory = Paths.get(this.jobsLocationsProperties.getArchives()).resolve(id);
        // TODO: This is flipped during V4 migration and should be changed back once clients are fixed
        Assertions.assertThat(Files.exists(archiveDirectory)).isTrue();
        Assertions.assertThat(Files.isDirectory(archiveDirectory)).isTrue();
    }


    @Test
    void canSubmitJobWithAttachments() throws Exception {
        Assumptions.assumeTrue(SystemUtils.IS_OS_UNIX);

        final List<ClusterCriteria> clusterCriteriaList = Lists.newArrayList(
            new ClusterCriteria(Sets.newHashSet(LOCALHOST_CLUSTER_TAG))
        );

        final String setUpFile = this.getResourceURI(BASE_DIR + "job" + FILE_DELIMITER + "jobsetupfile");

        final File attachment1File = this.resourceLoader
            .getResource(BASE_DIR + "job/query.sql")
            .getFile();

        final MockMultipartFile attachment1;
        try (InputStream is = new FileInputStream(attachment1File)) {
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
        try (InputStream is = new FileInputStream(attachment2File)) {
            attachment2 = new MockMultipartFile(
                "attachment",
                attachment2File.getName(),
                MediaType.APPLICATION_OCTET_STREAM_VALUE,
                is
            );
        }
        final Set<String> commandCriteria = Sets.newHashSet(BASH_COMMAND_TAG);
        final JobRequest jobRequest = new JobRequest.Builder(
            JOB_NAME,
            JOB_USER,
            JOB_VERSION,
            clusterCriteriaList,
            commandCriteria
        )
            .withCommandArgs(SLEEP_AND_ECHO_COMMAND_ARGS)
            .withDisableLogArchival(true)
            .withSetupFile(setUpFile)
            .withDescription(JOB_DESCRIPTION)
            .build();

        this.waitForDone(this.submitJob(4, jobRequest, Lists.newArrayList(attachment1, attachment2)));
    }

    @Test
    void testSubmitJobMethodMissingCluster() throws Exception {
        Assumptions.assumeTrue(SystemUtils.IS_OS_UNIX);

        final List<ClusterCriteria> clusterCriteriaList = new ArrayList<>();
        final Set<String> clusterTags = Sets.newHashSet("undefined");
        final ClusterCriteria clusterCriteria = new ClusterCriteria(clusterTags);
        clusterCriteriaList.add(clusterCriteria);

        final String jobId = UUID.randomUUID().toString();

        final Set<String> commandCriteria = Sets.newHashSet(BASH_COMMAND_TAG);
        final JobRequest jobRequest = new JobRequest.Builder(
            JOB_NAME,
            JOB_USER,
            JOB_VERSION,
            clusterCriteriaList,
            commandCriteria
        )
            .withId(jobId)
            .withCommandArgs(SLEEP_AND_ECHO_COMMAND_ARGS)
            .withDisableLogArchival(true)
            .build();

        RestAssured
            .given(this.getRequestSpecification())
            .contentType(MediaType.APPLICATION_JSON_VALUE)
            .accept(MediaType.APPLICATION_JSON_VALUE)
            .body(GenieObjectMapper.getMapper().writeValueAsBytes(jobRequest))
            .when()
            .port(this.port)
            .post(JOBS_API)
            .then()
            .statusCode(Matchers.is(HttpStatus.PRECONDITION_FAILED.value()))
            .contentType(Matchers.startsWith(MediaType.APPLICATION_JSON_VALUE))
            .body(
                EXCEPTION_MESSAGE_PATH,
                Matchers.anyOf(
                    Matchers.containsString(
                        "No cluster/command combination found for the given criteria. Unable to continue"
                    ),
                    Matchers.containsString("No cluster selected given criteria for job"),
                    Matchers.containsString("No clusters available to run any candidate command on")
                )
            )
            .body("stackTrace", Matchers.nullValue());

        Assertions.assertThat(this.getStatus(jobId)).isEqualByComparingTo(JobStatus.FAILED);
    }

    @Test
    void testSubmitJobMethodInvalidClusterCriteria() throws Exception {
        Assumptions.assumeTrue(SystemUtils.IS_OS_UNIX);

        final List<ClusterCriteria> clusterCriteriaList
            = Lists.newArrayList(new ClusterCriteria(Sets.newHashSet(" ", "", null)));

        final String jobId = UUID.randomUUID().toString();

        final Set<String> commandCriteria = Sets.newHashSet("bash");
        final JobRequest jobRequest = new JobRequest.Builder(
            JOB_NAME,
            JOB_USER,
            JOB_VERSION,
            clusterCriteriaList,
            commandCriteria
        )
            .withId(jobId)
            .withCommandArgs(SLEEP_AND_ECHO_COMMAND_ARGS)
            .withDisableLogArchival(true)
            .build();

        RestAssured
            .given(this.getRequestSpecification())
            .contentType(MediaType.APPLICATION_JSON_VALUE)
            .body(GenieObjectMapper.getMapper().writeValueAsBytes(jobRequest))
            .when()
            .port(this.port)
            .post(JOBS_API)
            .then()
            .statusCode(Matchers.is(HttpStatus.PRECONDITION_FAILED.value()));

        RestAssured
            .given(this.getRequestSpecification())
            .when()
            .port(this.port)
            .get(JOBS_API + "/{id}/status", jobId)
            .then()
            .statusCode(Matchers.is(HttpStatus.NOT_FOUND.value()));
    }

    @Test
    void testSubmitJobMethodInvalidCommandCriteria() throws Exception {
        Assumptions.assumeTrue(SystemUtils.IS_OS_UNIX);

        final List<ClusterCriteria> clusterCriteriaList
            = Lists.newArrayList(new ClusterCriteria(Sets.newHashSet("ok")));

        final String jobId = UUID.randomUUID().toString();

        final Set<String> commandCriteria = Sets.newHashSet(" ", "", null);
        final JobRequest jobRequest = new JobRequest.Builder(
            JOB_NAME,
            JOB_USER,
            JOB_VERSION,
            clusterCriteriaList,
            commandCriteria
        )
            .withId(jobId)
            .withCommandArgs(SLEEP_AND_ECHO_COMMAND_ARGS)
            .withDisableLogArchival(true)
            .build();

        RestAssured
            .given(this.getRequestSpecification())
            .contentType(MediaType.APPLICATION_JSON_VALUE)
            .body(GenieObjectMapper.getMapper().writeValueAsBytes(jobRequest))
            .when()
            .port(this.port)
            .post(JOBS_API)
            .then()
            .statusCode(Matchers.is(HttpStatus.PRECONDITION_FAILED.value()));

        RestAssured
            .given(this.getRequestSpecification())
            .when()
            .port(this.port)
            .get(JOBS_API + "/{id}/status", jobId)
            .then()
            .statusCode(Matchers.is(HttpStatus.NOT_FOUND.value()));
    }

    @Test
    void testSubmitJobMethodMissingCommand() throws Exception {
        Assumptions.assumeTrue(SystemUtils.IS_OS_UNIX);

        final List<ClusterCriteria> clusterCriteriaList = new ArrayList<>();
        final Set<String> clusterTags = Sets.newHashSet(LOCALHOST_CLUSTER_TAG);
        final ClusterCriteria clusterCriteria = new ClusterCriteria(clusterTags);
        clusterCriteriaList.add(clusterCriteria);

        final String jobId = UUID.randomUUID().toString();

        final Set<String> commandCriteria = Sets.newHashSet("undefined");
        final JobRequest jobRequest = new JobRequest.Builder(
            JOB_NAME,
            JOB_USER,
            JOB_VERSION,
            clusterCriteriaList,
            commandCriteria
        )
            .withId(jobId)
            .withCommandArgs(SLEEP_AND_ECHO_COMMAND_ARGS)
            .withDisableLogArchival(true)
            .build();

        RestAssured
            .given(this.getRequestSpecification())
            .contentType(MediaType.APPLICATION_JSON_VALUE)
            .body(GenieObjectMapper.getMapper().writeValueAsBytes(jobRequest))
            .when()
            .port(this.port)
            .post(JOBS_API)
            .then()
            .statusCode(Matchers.is(HttpStatus.PRECONDITION_FAILED.value()));

        Assertions.assertThat(this.getStatus(jobId)).isEqualByComparingTo(JobStatus.FAILED);
    }

    @Test
    void testSubmitJobMethodKill() throws Exception {
        Assumptions.assumeTrue(SystemUtils.IS_OS_UNIX);

        final List<ClusterCriteria> clusterCriteriaList = new ArrayList<>();
        final Set<String> clusterTags = Sets.newHashSet(LOCALHOST_CLUSTER_TAG);
        final ClusterCriteria clusterCriteria = new ClusterCriteria(clusterTags);
        clusterCriteriaList.add(clusterCriteria);

        final Set<String> commandCriteria = Sets.newHashSet(BASH_COMMAND_TAG);
        final JobRequest jobRequest = new JobRequest.Builder(
            JOB_NAME,
            JOB_USER,
            JOB_VERSION,
            clusterCriteriaList,
            commandCriteria
        )
            .withCommandArgs(SLEEP_60_COMMAND_ARGS)
            .withDisableLogArchival(true)
            .build();

        final String jobId = this.getIdFromLocation(
            RestAssured
                .given(this.getRequestSpecification())
                .contentType(MediaType.APPLICATION_JSON_VALUE)
                .body(GenieObjectMapper.getMapper().writeValueAsBytes(jobRequest))
                .when()
                .port(this.port)
                .post(JOBS_API)
                .then()
                .statusCode(Matchers.is(HttpStatus.ACCEPTED.value()))
                .header(HttpHeaders.LOCATION, Matchers.notNullValue())
                .extract()
                .header(HttpHeaders.LOCATION)
        );

        this.waitForRunning(jobId);

        // Make sure we can get output for a running job
        final ValidatableResponse outputResponse = RestAssured
            .given(this.getRequestSpecification())
            .accept(MediaType.APPLICATION_JSON_VALUE)
            .when()
            .port(this.port)
            .get(JOBS_API + "/{id}/output/{filePath}", jobId, "")
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.startsWith(MediaType.APPLICATION_JSON_VALUE))
            .body("parent", Matchers.is(Matchers.emptyOrNullString()))
            .body("directories[0].name", Matchers.is("genie/"));

        outputResponse
            .body("files[0].name", Matchers.is("run"))
            .body("files[1].name", Matchers.is("stderr"))
            .body("files[2].name", Matchers.is("stdout"));

        RestAssured
            .given(this.getRequestSpecification())
            .when()
            .port(this.port)
            .get(JOBS_API + "/{id}/output/{filePath}", jobId, "stdout")
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.containsString(ContentType.TEXT.toString()));

        // Let it run for a couple of seconds
        Thread.sleep(2000);

        // Send a kill request to the job.
        final RestDocumentationFilter killResultFilter = RestAssuredRestDocumentation.document(
            "{class-name}/killJob/",
            Snippets.ID_PATH_PARAM
        );

        RestAssured
            .given(this.getRequestSpecification())
            .filter(killResultFilter)
            .when()
            .port(this.port)
            .delete(JOBS_API + "/{id}", jobId)
            .then()
            .statusCode(Matchers.is(HttpStatus.ACCEPTED.value()));

        this.waitForDone(jobId);

        RestAssured
            .given(this.getRequestSpecification())
            .when()
            .port(this.port)
            .get(JOBS_API + "/{id}", jobId)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.containsString(MediaTypes.HAL_JSON_VALUE))
            .body(ID_PATH, Matchers.is(jobId))
            .body(STATUS_PATH, Matchers.is(JobStatus.KILLED.toString()))
            .body(STATUS_MESSAGE_PATH, Matchers.is(JobStatusMessages.JOB_KILLED_BY_USER));

        // Kill the job again to make sure it doesn't cause a problem.
        RestAssured
            .given(this.getRequestSpecification())
            .filter(killResultFilter)
            .when()
            .port(this.port)
            .delete(JOBS_API + "/{id}", jobId)
            .then()
            .statusCode(Matchers.is(HttpStatus.ACCEPTED.value()));
    }

    @Test
    void testSubmitJobMethodKillOnTimeout() throws Exception {
        Assumptions.assumeTrue(SystemUtils.IS_OS_UNIX);

        final List<ClusterCriteria> clusterCriteriaList = new ArrayList<>();
        final Set<String> clusterTags = Sets.newHashSet(LOCALHOST_CLUSTER_TAG);
        final ClusterCriteria clusterCriteria = new ClusterCriteria(clusterTags);
        clusterCriteriaList.add(clusterCriteria);

        final Set<String> commandCriteria = Sets.newHashSet(BASH_COMMAND_TAG);
        final JobRequest.Builder builder = new JobRequest.Builder(
            JOB_NAME,
            JOB_USER,
            JOB_VERSION,
            clusterCriteriaList,
            commandCriteria
        )
            .withTimeout(1)
            .withDisableLogArchival(true)
            .withCommandArgs(SLEEP_60_COMMAND_ARGS);

        final JobRequest jobRequest = builder.build();

        final String id = this.getIdFromLocation(
            RestAssured
                .given(this.getRequestSpecification())
                .contentType(MediaType.APPLICATION_JSON_VALUE)
                .body(GenieObjectMapper.getMapper().writeValueAsBytes(jobRequest))
                .when()
                .port(this.port)
                .post(JOBS_API)
                .then()
                .statusCode(Matchers.is(HttpStatus.ACCEPTED.value()))
                .header(HttpHeaders.LOCATION, Matchers.notNullValue())
                .extract()
                .header(HttpHeaders.LOCATION)
        );

        this.waitForDone(id);

        RestAssured
            .given(this.getRequestSpecification())
            .when()
            .port(this.port)
            .get(JOBS_API + "/{id}", id)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.containsString(MediaTypes.HAL_JSON_VALUE))
            .body(ID_PATH, Matchers.is(id))
            .body(STATUS_PATH, Matchers.is(JobStatus.KILLED.toString()))
            .body(STATUS_MESSAGE_PATH, Matchers.is(JobStatusMessages.JOB_EXCEEDED_TIMEOUT));
    }

    @Test
    void testKillJobImmediately() throws Exception {
        Assumptions.assumeTrue(SystemUtils.IS_OS_UNIX);

        final List<ClusterCriteria> clusterCriteriaList = new ArrayList<>();
        final Set<String> clusterTags = Sets.newHashSet(LOCALHOST_CLUSTER_TAG);
        final ClusterCriteria clusterCriteria = new ClusterCriteria(clusterTags);
        clusterCriteriaList.add(clusterCriteria);

        final Set<String> commandCriteria = Sets.newHashSet(BASH_COMMAND_TAG);
        final JobRequest jobRequest = new JobRequest.Builder(
            JOB_NAME,
            JOB_USER,
            JOB_VERSION,
            clusterCriteriaList,
            commandCriteria
        )
            .withCommandArgs(SLEEP_60_COMMAND_ARGS)
            .withDisableLogArchival(true)
            .build();

        final String jobId = this.getIdFromLocation(
            RestAssured
                .given(this.getRequestSpecification())
                .contentType(MediaType.APPLICATION_JSON_VALUE)
                .body(GenieObjectMapper.getMapper().writeValueAsBytes(jobRequest))
                .when()
                .port(this.port)
                .post(JOBS_API)
                .then()
                .statusCode(Matchers.is(HttpStatus.ACCEPTED.value()))
                .header(HttpHeaders.LOCATION, Matchers.notNullValue())
                .extract()
                .header(HttpHeaders.LOCATION)
        );

//        // Let it run for a couple of seconds
//        Thread.sleep(2000);

        RestAssured
            .given(this.getRequestSpecification())
            .when()
            .port(this.port)
            .delete(JOBS_API + "/{id}", jobId)
            .then()
            .statusCode(Matchers.is(HttpStatus.ACCEPTED.value()));

        this.waitForDone(jobId);

        RestAssured
            .given(this.getRequestSpecification())
            .when()
            .port(this.port)
            .get(JOBS_API + "/{id}", jobId)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.containsString(MediaTypes.HAL_JSON_VALUE))
            .body(ID_PATH, Matchers.is(jobId))
            .body(STATUS_PATH, Matchers.is(JobStatus.KILLED.toString()))
            .body(STATUS_MESSAGE_PATH, Matchers.is(JobStatusMessages.JOB_KILLED_BY_USER));

        // Kill the job again to make sure it doesn't cause a problem.
        RestAssured
            .given(this.getRequestSpecification())
            .when()
            .port(this.port)
            .delete(JOBS_API + "/{id}", jobId)
            .then()
            .statusCode(Matchers.is(HttpStatus.ACCEPTED.value()));
    }

    @Test
    void testSubmitJobMethodFailure() throws Exception {
        Assumptions.assumeTrue(SystemUtils.IS_OS_UNIX);
        final List<String> commandArgs;
        commandArgs = Lists.newArrayList("-c", "sleep 3 && exit 1");

        final List<ClusterCriteria> clusterCriteriaList = new ArrayList<>();
        final Set<String> clusterTags = Sets.newHashSet(LOCALHOST_CLUSTER_TAG);
        final ClusterCriteria clusterCriteria = new ClusterCriteria(clusterTags);
        clusterCriteriaList.add(clusterCriteria);

        final Set<String> commandCriteria = Sets.newHashSet(BASH_COMMAND_TAG);
        final JobRequest jobRequest = new JobRequest.Builder(
            JOB_NAME,
            JOB_USER,
            JOB_VERSION,
            clusterCriteriaList,
            commandCriteria
        )
            .withCommandArgs(commandArgs)
            .withDisableLogArchival(true)
            .build();

        final String id = this.getIdFromLocation(
            RestAssured
                .given(this.getRequestSpecification())
                .contentType(MediaType.APPLICATION_JSON_VALUE)
                .body(GenieObjectMapper.getMapper().writeValueAsBytes(jobRequest))
                .when()
                .port(this.port)
                .post(JOBS_API)
                .then()
                .statusCode(Matchers.is(HttpStatus.ACCEPTED.value()))
                .header(HttpHeaders.LOCATION, Matchers.notNullValue())
                .extract()
                .header(HttpHeaders.LOCATION)
        );

        this.waitForDone(id);

        Assertions.assertThat(this.getStatus(id)).isEqualByComparingTo(JobStatus.FAILED);

        RestAssured
            .given(this.getRequestSpecification())
            .when()
            .port(this.port)
            .get(JOBS_API + "/{id}", id)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.containsString(MediaTypes.HAL_JSON_VALUE))
            .body(ID_PATH, Matchers.is(id))
            .body(STATUS_PATH, Matchers.is(JobStatus.FAILED.toString()))
            .body(STATUS_MESSAGE_PATH, Matchers.is(JobStatusMessages.JOB_FAILED));
    }

    @Test
    void testResponseContentType() throws Exception {
        Assumptions.assumeTrue(SystemUtils.IS_OS_UNIX);
        final String utf8 = "UTF-8";

        final JobRequest jobRequest = new JobRequest.Builder(
            JOB_NAME,
            JOB_USER,
            JOB_VERSION,
            Lists.newArrayList(new ClusterCriteria(Sets.newHashSet("localhost"))),
            Sets.newHashSet("bash")
        )
            .withCommandArgs(SLEEP_AND_ECHO_COMMAND_ARGS)
            .build();

        final String jobId = this.getIdFromLocation(
            RestAssured
                .given(this.getRequestSpecification())
                .contentType(MediaType.APPLICATION_JSON_VALUE)
                .body(GenieObjectMapper.getMapper().writeValueAsBytes(jobRequest))
                .when()
                .port(this.port)
                .post(JOBS_API)
                .then()
                .statusCode(Matchers.is(HttpStatus.ACCEPTED.value()))
                .header(HttpHeaders.LOCATION, Matchers.notNullValue())
                .extract()
                .header(HttpHeaders.LOCATION)
        );

        this.waitForRunning(jobId);

        RestAssured
            .given(this.getRequestSpecification())
            .accept(MediaType.ALL_VALUE)
            .when()
            .port(this.port)
            .get(JOBS_API + "/" + jobId + "/output/stdout")
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.containsString(MediaType.TEXT_PLAIN_VALUE))
            .contentType(Matchers.containsString(utf8));

        RestAssured
            .given(this.getRequestSpecification())
            .accept(MediaType.ALL_VALUE)
            .when()
            .port(this.port)
            .get(JOBS_API + "/" + jobId + "/output/stderr")
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.containsString(MediaType.TEXT_PLAIN_VALUE))
            .contentType(Matchers.containsString(utf8));

        // Verify the file is served as UTF-8 even if it's not
        RestAssured
            .given(this.getRequestSpecification())
            .accept(MediaType.ALL_VALUE)
            .when()
            .port(this.port)
            .get(JOBS_API + "/" + jobId + "/output/genie/command/" + CMD1_ID + "/config/" + GB18030_TXT)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.containsString(MediaType.TEXT_PLAIN_VALUE))
            .contentType(Matchers.containsString(utf8));
    }

    @Test
    void testJobNotFound() {
        Assertions.assertThat(this.jobRepository.count()).isEqualTo(0L);

        final String jobId = UUID.randomUUID().toString();

        final ArrayList<String> paths = Lists.newArrayList(
            "",
            "/request",
            "/execution",
            "/status",
            "/metadata"
        );

        for (final String path : paths) {
            RestAssured
                .given(this.getRequestSpecification())
                .when()
                .port(this.port)
                .get(JOBS_API + "/{id}" + path, jobId)
                .then()
                .statusCode(Matchers.is(HttpStatus.NOT_FOUND.value()))
                .contentType(Matchers.startsWith(MediaType.APPLICATION_JSON_VALUE))
                .body(EXCEPTION_MESSAGE_PATH, Matchers.startsWith("No job"))
                .body(EXCEPTION_MESSAGE_PATH, Matchers.containsString(jobId));
        }

        // TODO '/output' behaves differently than the rest.
        // The resource handler composes the response, including in case of error, rather than just throwing.
        // Would be nice to align this. Hard for now due to support of both V3 and V4 logic.
        RestAssured
            .given(this.getRequestSpecification())
            .when()
            .port(this.port)
            .get(JOBS_API + "/{id}/output", jobId)
            .then()
            .statusCode(Matchers.is(HttpStatus.NOT_FOUND.value()))
            .contentType(Matchers.startsWith(MediaType.APPLICATION_JSON_VALUE))
            .body(EXCEPTION_MESSAGE_PATH, Matchers.startsWith("No job with id " + jobId));
    }

    @Test
    void testFileNotFound() throws Exception {
        Assumptions.assumeTrue(SystemUtils.IS_OS_UNIX);
        final String utf8 = "UTF-8";

        final JobRequest jobRequest = new JobRequest.Builder(
            JOB_NAME,
            JOB_USER,
            JOB_VERSION,
            Lists.newArrayList(new ClusterCriteria(Sets.newHashSet("localhost"))),
            Sets.newHashSet("bash")
        )
            .withCommandArgs(SLEEP_AND_ECHO_COMMAND_ARGS)
            .build();

        final String jobId = this.getIdFromLocation(
            RestAssured
                .given(this.getRequestSpecification())
                .contentType(MediaType.APPLICATION_JSON_VALUE)
                .body(GenieObjectMapper.getMapper().writeValueAsBytes(jobRequest))
                .when()
                .port(this.port)
                .post(JOBS_API)
                .then()
                .statusCode(Matchers.is(HttpStatus.ACCEPTED.value()))
                .header(HttpHeaders.LOCATION, Matchers.notNullValue())
                .extract()
                .header(HttpHeaders.LOCATION)
        );

        this.waitForRunning(jobId);

        RestAssured
            .given(this.getRequestSpecification())
            .accept(MediaType.ALL_VALUE)
            .when()
            .port(this.port)
            .get(JOBS_API + "/" + jobId + "/output/stdout")
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.containsString(MediaType.TEXT_PLAIN_VALUE))
            .contentType(Matchers.containsString(utf8));

        RestAssured
            .given(this.getRequestSpecification())
            .accept(MediaType.ALL_VALUE)
            .when()
            .port(this.port)
            .get(JOBS_API + "/" + jobId + "/output/" + UUID.randomUUID().toString())
            .then()
            .statusCode(Matchers.is(HttpStatus.NOT_FOUND.value()));
    }

    private JobStatus getStatus(final String jobId) throws IOException {
        final String statusString = RestAssured
            .given(this.getRequestSpecification())
            .when()
            .port(this.port)
            .get(JOBS_API + "/{id}/status", jobId)
            .asString();

        return JobStatus.valueOf(GenieObjectMapper.getMapper().readTree(statusString).get("status").textValue());
    }

    private void waitForDone(final String jobId) throws Exception {
        int counter = 0;
        while (true) {
            final JobStatus status = this.getStatus(jobId);
            if (status.isActive()) {
                LOG.info("Iteration {} sleeping for {} ms", counter, SLEEP_TIME);
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
            final JobStatus status = this.getStatus(jobId);
            if (status != JobStatus.RUNNING && !status.isFinished()) {
                LOG.info("Iteration {} sleeping for {} ms", counter, SLEEP_TIME);
                Thread.sleep(SLEEP_TIME);
                counter++;
            } else {
                break;
            }
        }
    }

    private void linkAllEntities() throws Exception {
        final List<String> apps = new ArrayList<>();
        apps.add(APP1_ID);
        apps.add(APP2_ID);

        RestAssured
            .given(this.getRequestSpecification())
            .contentType(MediaType.APPLICATION_JSON_VALUE)
            .body(GenieObjectMapper.getMapper().writeValueAsBytes(apps))
            .when()
            .port(this.port)
            .post(COMMANDS_API + FILE_DELIMITER + CMD1_ID + FILE_DELIMITER + APPLICATIONS_LINK_KEY)
            .then()
            .statusCode(Matchers.is(HttpStatus.NO_CONTENT.value()));
    }

    private void createAnApplication(final String id, final String appName) throws Exception {
        final String setUpFile = this.getResourceURI(BASE_DIR + id + FILE_DELIMITER + "setupfile");

        final String depFile1 = this.getResourceURI(BASE_DIR + id + FILE_DELIMITER + "dep1");
        final String depFile2 = this.getResourceURI(BASE_DIR + id + FILE_DELIMITER + "dep2");
        final Set<String> app1Dependencies = Sets.newHashSet(depFile1, depFile2);

        final String configFile1 = this.getResourceURI(BASE_DIR + id + FILE_DELIMITER + "config1");
        final String configFile2 = this.getResourceURI(BASE_DIR + id + FILE_DELIMITER + "config2");
        final Set<String> app1Configs = Sets.newHashSet(configFile1, configFile2);

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

        RestAssured
            .given(this.getRequestSpecification())
            .contentType(MediaType.APPLICATION_JSON_VALUE)
            .body(GenieObjectMapper.getMapper().writeValueAsBytes(app))
            .when()
            .port(this.port)
            .post(APPLICATIONS_API)
            .then()
            .statusCode(Matchers.is(HttpStatus.CREATED.value()))
            .header(HttpHeaders.LOCATION, Matchers.notNullValue());
    }

    private void createAllClusters() throws Exception {
        final String setUpFile = this.getResourceURI(BASE_DIR + CLUSTER1_ID + FILE_DELIMITER + "setupfile");

        final String configFile1 = this.getResourceURI(BASE_DIR + CLUSTER1_ID + FILE_DELIMITER + "config1");
        final String configFile2 = this.getResourceURI(BASE_DIR + CLUSTER1_ID + FILE_DELIMITER + "config2");
        final Set<String> configs = Sets.newHashSet(configFile1, configFile2);

        final String depFile1 = this.getResourceURI(BASE_DIR + CLUSTER1_ID + FILE_DELIMITER + "dep1");
        final String depFile2 = this.getResourceURI(BASE_DIR + CLUSTER1_ID + FILE_DELIMITER + "dep2");
        final Set<String> clusterDependencies = Sets.newHashSet(depFile1, depFile2);
        final Set<String> tags = Sets.newHashSet(LOCALHOST_CLUSTER_TAG);

        final Cluster cluster = new Cluster.Builder(
            CLUSTER1_NAME,
            CLUSTER1_USER,
            CLUSTER1_VERSION,
            ClusterStatus.UP
        )
            .withId(CLUSTER1_ID)
            .withSetupFile(setUpFile)
            .withConfigs(configs)
            .withDependencies(clusterDependencies)
            .withTags(tags)
            .build();

        RestAssured
            .given(this.getRequestSpecification())
            .contentType(MediaType.APPLICATION_JSON_VALUE)
            .body(GenieObjectMapper.getMapper().writeValueAsBytes(cluster))
            .when()
            .port(this.port)
            .post(CLUSTERS_API)
            .then()
            .statusCode(Matchers.is(HttpStatus.CREATED.value()))
            .header(HttpHeaders.LOCATION, Matchers.notNullValue());
    }

    private void createAllCommands() throws Exception {
        final String setUpFile = this.getResourceURI(BASE_DIR + CMD1_ID + FILE_DELIMITER + "setupfile");

        final String configFile1 = this.getResourceURI(BASE_DIR + CMD1_ID + FILE_DELIMITER + "config1");
        final String configFile2 = this.getResourceURI(BASE_DIR + CMD1_ID + FILE_DELIMITER + "config2");
        final String configFile3 = this.getResourceURI(BASE_DIR + CMD1_ID + FILE_DELIMITER + GB18030_TXT);
        final Set<String> configs = Sets.newHashSet(configFile1, configFile2, configFile3);
        final String depFile1 = this.getResourceURI(BASE_DIR + CLUSTER1_ID + FILE_DELIMITER + "dep1");
        final String depFile2 = this.getResourceURI(BASE_DIR + CLUSTER1_ID + FILE_DELIMITER + "dep2");
        final Set<String> commandDependencies = Sets.newHashSet(depFile1, depFile2);

        final Set<String> tags = Sets.newHashSet(BASH_COMMAND_TAG);

        final Command cmd = new Command.Builder(
            CMD1_NAME,
            CMD1_USER,
            CMD1_VERSION,
            CommandStatus.ACTIVE,
            CMD1_EXECUTABLE_AND_ARGS,
            CHECK_DELAY
        )
            .withId(CMD1_ID)
            .withSetupFile(setUpFile)
            .withConfigs(configs)
            .withDependencies(commandDependencies)
            .withTags(tags)
            .withClusterCriteria(
                Lists.newArrayList(
                    new Criterion.Builder().withTags(Sets.newHashSet(LOCALHOST_CLUSTER_TAG)).build()
                )
            )
            .build();

        RestAssured
            .given(this.getRequestSpecification())
            .contentType(MediaType.APPLICATION_JSON_VALUE)
            .body(GenieObjectMapper.getMapper().writeValueAsBytes(cmd))
            .when()
            .port(this.port)
            .post(COMMANDS_API)
            .then()
            .statusCode(Matchers.is(HttpStatus.CREATED.value()))
            .header(HttpHeaders.LOCATION, Matchers.notNullValue());
    }

    private String getResourceURI(final String location) throws IOException {
        return this.resourceLoader.getResource(location).getURI().toString();
    }

    private String getExpectedRunContents(
        final String runFileContents,
        final String jobWorkingDir,
        final String jobId
    ) {
        return runFileContents
            .replaceAll("TEST_GENIE_JOB_WORKING_DIR_PLACEHOLDER", jobWorkingDir)
            .replaceAll("JOB_ID_PLACEHOLDER", jobId);
    }
}
