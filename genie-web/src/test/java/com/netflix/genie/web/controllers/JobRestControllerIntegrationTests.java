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
import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.dto.JobStatusMessages;
import com.netflix.genie.common.internal.util.GenieHostInfo;
import com.netflix.genie.common.util.GenieObjectMapper;
import io.restassured.RestAssured;
import io.restassured.specification.RequestSpecification;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
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
import org.springframework.restdocs.payload.PayloadDocumentation;
import org.springframework.restdocs.request.RequestDocumentation;
import org.springframework.restdocs.restassured3.RestAssuredRestDocumentation;
import org.springframework.restdocs.restassured3.RestDocumentationFilter;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.file.Files;
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
@Slf4j
public class JobRestControllerIntegrationTests extends RestControllerIntegrationTestsBase {

    private static final long SLEEP_TIME = 1000L;
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
    private static final String JOB_COMMAND_LINK_PATH = "_links.command.href";
    private static final String JOB_CLUSTER_LINK_PATH = "_links.cluster.href";
    private static final String JOB_APPLICATIONS_LINK_PATH = "_links.applications.href";

    private static final long CHECK_DELAY = 500L;

    private static final String BASE_DIR
        = "com/netflix/genie/web/controllers/JobRestControllerIntegrationTests/";
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
    private static final String CMD1_TAGS
        = BASH_COMMAND_TAG + ","
        + "genie.id:" + CMD1_ID + ","
        + "genie.name:" + CMD1_NAME;

    private static final String CLUSTER1_ID = "cluster1";
    private static final String CLUSTER1_NAME = "Local laptop";
    private static final String CLUSTER1_USER = "genie";
    private static final String CLUSTER1_VERSION = "1.0";
    private static final String CLUSTER1_TAGS
        = "genie.id:" + CLUSTER1_ID + ","
        + "genie.name:" + CLUSTER1_NAME + ","
        + LOCALHOST_CLUSTER_TAG;
    // This file is not UTF-8 encoded. It is uploaded to test server behavior
    // related to charset headers
    private static final String GB18030_TXT = "GB18030.txt";

    private ResourceLoader resourceLoader;
    private JsonNode metadata;
    private String schedulerJobName;
    private String schedulerRunId;

    @Autowired
    private GenieHostInfo genieHostInfo;

    @Autowired
    private Resource jobDirResource;

    @Value("${genie.file.cache.location}")
    private String baseCacheLocation;

    /**
     * {@inheritDoc}
     */
    @Before
    @Override
    public void setup() throws Exception {
        super.setup();

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

    /**
     * {@inheritDoc}
     */
    @After
    @Override
    public void cleanup() throws Exception {
        super.cleanup();
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
        final List<String> commandArgs = Lists.newArrayList("-c", "'echo hello world'");

        final String clusterTag = LOCALHOST_CLUSTER_TAG;
        final List<ClusterCriteria> clusterCriteriaList = Lists.newArrayList(
            new ClusterCriteria(Sets.newHashSet(clusterTag))
        );

        final String setUpFile = this.resourceLoader
            .getResource(BASE_DIR + "job" + FILE_DELIMITER + "jobsetupfile")
            .getFile()
            .getAbsolutePath();

        final String configFile1 = this.resourceLoader
            .getResource(BASE_DIR + "job" + FILE_DELIMITER + "config1")
            .getFile()
            .getAbsolutePath();
        final Set<String> configs = Sets.newHashSet(configFile1);

        final String depFile1 = this.resourceLoader
            .getResource(BASE_DIR + "job" + FILE_DELIMITER + "dep1")
            .getFile()
            .getAbsolutePath();
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
            .withCommandArgs(commandArgs)
            .withDisableLogArchival(true)
            .withSetupFile(setUpFile)
            .withConfigs(configs)
            .withDependencies(dependencies)
            .withDescription(JOB_DESCRIPTION)
            .withMetadata(this.metadata)
            .build();

        final String id = this.submitJob(documentationId, jobRequest, null);
        this.waitForDone(id);

        this.checkJobStatus(documentationId, id);
        this.checkJob(documentationId, id, commandArgs);
        this.checkJobOutput(documentationId, id);
        this.checkJobRequest(
            documentationId, id, commandArgs, setUpFile, clusterTag, commandTag, configFile1, depFile1
        );
        this.checkJobExecution(documentationId, id);
        this.checkJobMetadata(documentationId, id);
        this.checkJobCluster(documentationId, id);
        this.checkJobCommand(documentationId, id);
        this.checkJobApplications(documentationId, id);
        this.checkFindJobs(documentationId, id, JOB_USER);

        Assert.assertThat(this.jobRepository.count(), Matchers.is(1L));

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

    private void checkJob(final int documentationId, final String id, final List<String> commandArgs) {
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
            .contentType(Matchers.is(MediaTypes.HAL_JSON_UTF8_VALUE))
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
            .body(ARCHIVE_LOCATION_PATH, Matchers.isEmptyOrNullString())
            .body(CLUSTER_NAME_PATH, Matchers.is(CLUSTER1_NAME))
            .body(COMMAND_NAME_PATH, Matchers.is(CMD1_NAME))
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
        RestAssured
            .given(this.getRequestSpecification())
            .filter(jsonResultFilter)
            .accept(MediaType.APPLICATION_JSON_VALUE)
            .when()
            .port(this.port)
            .get(JOBS_API + "/{id}/output/{filePath}", id, "")
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.equalToIgnoringCase(MediaType.APPLICATION_JSON_UTF8_VALUE))
            .body("parent", Matchers.isEmptyOrNullString())
            .body("directories[0].name", Matchers.is("genie/"))
            .body("files[0].name", Matchers.is("config1"))
            .body("files[1].name", Matchers.is("dep1"))
            .body("files[2].name", Matchers.is("jobsetupfile"))
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

        // Check getting a directory as HTML
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

        // check that the generated run file is correct
        final String runShFileName = SystemUtils.IS_OS_LINUX ? "linux-runsh.txt" : "non-linux-runsh.txt";

        final String runShFile = this.resourceLoader
            .getResource(BASE_DIR + runShFileName)
            .getFile()
            .getAbsolutePath();
        final String runFileContents = new String(Files.readAllBytes(Paths.get(runShFile)), "UTF-8");

        final String jobWorkingDir = this.jobDirResource.getFile().getCanonicalPath() + FILE_DELIMITER + id;
        final String expectedRunScriptContent = this.getExpectedRunContents(runFileContents, jobWorkingDir, id);

        RestAssured
            .given(this.getRequestSpecification())
            .filter(fileResultFilter)
            .when()
            .port(this.port)
            .get(JOBS_API + "/{id}/output/{filePath}", id, "run")
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .body(Matchers.is(expectedRunScriptContent));
    }

    private void checkJobRequest(
        final int documentationId,
        final String id,
        final List<String> commandArgs,
        final String setupFile,
        final String clusterTag,
        final String commandTag,
        final String configFile1,
        final String depFile1
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
            .contentType(Matchers.is(MediaTypes.HAL_JSON_UTF8_VALUE))
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
            .body(DISABLE_LOG_ARCHIVAL_PATH, Matchers.is(true))
            .body(CONFIGS_PATH, Matchers.hasSize(1))
            .body(CONFIGS_PATH + "[0]", Matchers.is(configFile1))
            .body(DEPENDENCIES_PATH, Matchers.hasSize(1))
            .body(DEPENDENCIES_PATH + "[0]", Matchers.is(depFile1))
            .body(EMAIL_PATH, Matchers.nullValue())
            .body(CPU_PATH, Matchers.nullValue())
            .body(MEMORY_PATH, Matchers.nullValue())
            .body(APPLICATIONS_PATH, Matchers.empty());
    }

    private void checkJobExecution(final int documentationId, final String id) {
        final RestDocumentationFilter getResultFilter = RestAssuredRestDocumentation.document(
            "{class-name}/" + documentationId + "/getJobExecution/",
            Snippets.ID_PATH_PARAM, // Path parameters
            Snippets.HAL_CONTENT_TYPE_HEADER, // Response Headers
            Snippets.getJobExecutionResponsePayload(), // Response fields
            Snippets.JOB_EXECUTION_LINKS // Links
        );

        RestAssured
            .given(this.getRequestSpecification())
            .filter(getResultFilter)
            .when()
            .port(this.port)
            .get(JOBS_API + "/{id}/execution", id)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.is(MediaTypes.HAL_JSON_UTF8_VALUE))
            .body(ID_PATH, Matchers.is(id))
            .body(CREATED_PATH, Matchers.notNullValue())
            .body(UPDATED_PATH, Matchers.notNullValue())
            .body(HOST_NAME_PATH, Matchers.is(this.genieHostInfo.getHostname()))
            .body(PROCESS_ID_PATH, Matchers.notNullValue())
            .body(CHECK_DELAY_PATH, Matchers.is((int) CHECK_DELAY))
            .body(EXIT_CODE_PATH, Matchers.is(JobExecution.SUCCESS_EXIT_CODE));
    }

    private void checkJobMetadata(final int documentationId, final String id) {
        final RestDocumentationFilter getResultFilter = RestAssuredRestDocumentation.document(
            "{class-name}/" + documentationId + "/getJobMetadata/",
            Snippets.ID_PATH_PARAM, // Path parameters
            Snippets.HAL_CONTENT_TYPE_HEADER, // Response Headers
            Snippets.getJobMetadataResponsePayload(), // Response fields
            Snippets.JOB_METADATA_LINKS // Links
        );

        RestAssured
            .given(this.getRequestSpecification())
            .filter(getResultFilter)
            .when()
            .port(this.port)
            .get(JOBS_API + "/{id}/metadata", id)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.is(MediaTypes.HAL_JSON_UTF8_VALUE))
            .body(ID_PATH, Matchers.is(id))
            .body(CREATED_PATH, Matchers.notNullValue())
            .body(UPDATED_PATH, Matchers.notNullValue())
            .body(CLIENT_HOST_PATH, Matchers.notNullValue())
            .body(USER_AGENT_PATH, Matchers.notNullValue())
            .body(NUM_ATTACHMENTS_PATH, Matchers.notNullValue())
            .body(TOTAL_SIZE_ATTACHMENTS_PATH, Matchers.notNullValue())
            .body(STD_OUT_SIZE_PATH, Matchers.notNullValue())
            .body(STD_ERR_SIZE_PATH, Matchers.notNullValue());
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
            .contentType(Matchers.is(MediaTypes.HAL_JSON_UTF8_VALUE))
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
            .contentType(Matchers.is(MediaTypes.HAL_JSON_UTF8_VALUE))
            .body(ID_PATH, Matchers.is(CMD1_ID))
            .body(CREATED_PATH, Matchers.notNullValue())
            .body(UPDATED_PATH, Matchers.notNullValue())
            .body(NAME_PATH, Matchers.is(CMD1_NAME))
            .body(USER_PATH, Matchers.is(CMD1_USER))
            .body(VERSION_PATH, Matchers.is(CMD1_VERSION))
            .body("executable", Matchers.is(CMD1_EXECUTABLE));
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
            .contentType(Matchers.is(MediaTypes.HAL_JSON_UTF8_VALUE))
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
            .contentType(Matchers.is(MediaTypes.HAL_JSON_UTF8_VALUE))
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
            .statusCode(Matchers.is(HttpStatus.CONFLICT.value()));
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
        final List<String> commandArgs = Lists.newArrayList("-c", "'echo hello world'");

        final List<ClusterCriteria> clusterCriteriaList = Lists.newArrayList(
            new ClusterCriteria(Sets.newHashSet(LOCALHOST_CLUSTER_TAG))
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
        final List<String> commandArgs = Lists.newArrayList("-c", "'echo hello world'");

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
            .withCommandArgs(commandArgs)
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
        final List<String> commandArgs = Lists.newArrayList("-c", "'echo hello world'");

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
            .withCommandArgs(commandArgs)
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

    /**
     * Test the job submit method for incorrect cluster criteria.
     *
     * @throws Exception If there is a problem.
     */
    @Test
    public void testSubmitJobMethodInvalidCommandCriteria() throws Exception {
        Assume.assumeTrue(SystemUtils.IS_OS_UNIX);
        final List<String> commandArgs = Lists.newArrayList("-c", "'echo hello world'");

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
            .withCommandArgs(commandArgs)
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

    /**
     * Test the job submit method for incorrect command resolved.
     *
     * @throws Exception If there is a problem.
     */
    @Test
    public void testSubmitJobMethodMissingCommand() throws Exception {
        Assume.assumeTrue(SystemUtils.IS_OS_UNIX);
        final List<String> commandArgs = Lists.newArrayList("-c", "'echo hello world'");

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
            .withCommandArgs(commandArgs)
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
        final List<String> commandArgs = Lists.newArrayList("-c", "'sleep 60'");

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
            .contentType(Matchers.is(MediaTypes.HAL_JSON_UTF8_VALUE))
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

    /**
     * Test the job submit method for when the job is killed as it times out.
     *
     * @throws Exception If there is a problem.
     */
    @Test
    public void testSubmitJobMethodKillOnTimeout() throws Exception {
        Assume.assumeTrue(SystemUtils.IS_OS_UNIX);
        final List<String> commandArgs = Lists.newArrayList("-c", "'sleep 60'");

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
            .withTimeout(5)
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

        RestAssured
            .given(this.getRequestSpecification())
            .when()
            .port(this.port)
            .get(JOBS_API + "/{id}", id)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.is(MediaTypes.HAL_JSON_UTF8_VALUE))
            .body(ID_PATH, Matchers.is(id))
            .body(STATUS_PATH, Matchers.is(JobStatus.KILLED.toString()))
            .body(STATUS_MESSAGE_PATH, Matchers.is("Job exceeded timeout."));
    }

    /**
     * Test the job submit method for when the job fails.
     *
     * @throws Exception If there is a problem.
     */
    @Test
    public void testSubmitJobMethodFailure() throws Exception {
        Assume.assumeTrue(SystemUtils.IS_OS_UNIX);
        final List<String> commandArgs = Lists.newArrayList("-c", "'exit 1'");

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

        Assert.assertEquals(this.getStatus(id), "{\"status\":\"FAILED\"}");

        RestAssured
            .given(this.getRequestSpecification())
            .when()
            .port(this.port)
            .get(JOBS_API + "/{id}", id)
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.is(MediaTypes.HAL_JSON_UTF8_VALUE))
            .body(ID_PATH, Matchers.is(id))
            .body(STATUS_PATH, Matchers.is(JobStatus.FAILED.toString()))
            .body(STATUS_MESSAGE_PATH, Matchers.is(JobStatusMessages.JOB_FAILED));
    }

    /**
     * Test the response content types to ensure UTF-8.
     *
     * @throws Exception If there is a problem.
     */
    @Test
    public void testResponseContentType() throws Exception {
        Assume.assumeTrue(SystemUtils.IS_OS_UNIX);
        final List<String> commandArgs = Lists.newArrayList("-c", "'echo hello'");

        final JobRequest jobRequest = new JobRequest.Builder(
            JOB_NAME,
            JOB_USER,
            JOB_VERSION,
            Lists.newArrayList(new ClusterCriteria(Sets.newHashSet("localhost"))),
            Sets.newHashSet("bash")
        )
            .withCommandArgs(commandArgs)
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

        this.waitForDone(jobId);

        RestAssured
            .given(this.getRequestSpecification())
            .when()
            .port(this.port)
            .get(JOBS_API + "/" + jobId + "/output/genie/logs/env.log")
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.containsString(MediaType.TEXT_PLAIN_VALUE))
            .contentType(Matchers.containsString("UTF-8"));

        RestAssured
            .given(this.getRequestSpecification())
            .when()
            .port(this.port)
            .get(JOBS_API + "/" + jobId + "/output/genie/logs/genie.log")
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.containsString(MediaType.TEXT_PLAIN_VALUE))
            .contentType(Matchers.containsString("UTF-8"));

        RestAssured
            .given(this.getRequestSpecification())
            .when()
            .port(this.port)
            .get(JOBS_API + "/" + jobId + "/output/genie/genie.done")
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.containsString(MediaType.TEXT_PLAIN_VALUE))
            .contentType(Matchers.containsString("UTF-8"));

        RestAssured
            .given(this.getRequestSpecification())
            .accept(MediaType.ALL_VALUE)
            .when()
            .port(this.port)
            .get(JOBS_API + "/" + jobId + "/output/stdout")
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.containsString(MediaType.TEXT_PLAIN_VALUE))
            .contentType(Matchers.containsString("UTF-8"));

        RestAssured
            .given(this.getRequestSpecification())
            .accept(MediaType.ALL_VALUE)
            .when()
            .port(this.port)
            .get(JOBS_API + "/" + jobId + "/output/stderr")
            .then()
            .statusCode(Matchers.is(HttpStatus.OK.value()))
            .contentType(Matchers.containsString(MediaType.TEXT_PLAIN_VALUE))
            .contentType(Matchers.containsString("UTF-8"));

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
            .contentType(Matchers.containsString("UTF-8"));
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
            .replace("COMMAND_TAGS_PLACEHOLDER", CMD1_TAGS)
            .replace("CLUSTER_ID_PLACEHOLDER", CLUSTER1_ID)
            .replace("CLUSTER_NAME_PLACEHOLDER", CLUSTER1_NAME)
            .replace("CLUSTER_TAGS_PLACEHOLDER", CLUSTER1_TAGS);
    }

    private String getStatus(final String jobId) {
        return RestAssured
            .given(this.getRequestSpecification())
            .when()
            .port(this.port)
            .get(JOBS_API + "/{id}/status", jobId)
            .asString();
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

        final List<String> cmds = Lists.newArrayList(CMD1_ID);

        RestAssured
            .given(this.getRequestSpecification())
            .contentType(MediaType.APPLICATION_JSON_VALUE)
            .body(GenieObjectMapper.getMapper().writeValueAsBytes(cmds))
            .when()
            .port(this.port)
            .post(CLUSTERS_API + FILE_DELIMITER + CLUSTER1_ID + FILE_DELIMITER + COMMANDS_LINK_KEY)
            .then()
            .statusCode(Matchers.is(HttpStatus.NO_CONTENT.value()));
    }

    private void createAnApplication(final String id, final String appName) throws Exception {
        final String setUpFile = this.resourceLoader.getResource(
            BASE_DIR
                + id
                + FILE_DELIMITER
                + "setupfile"
        ).getFile().getAbsolutePath();

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
        final Set<String> app1Dependencies = Sets.newHashSet(depFile1, depFile2);

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
        final String setUpFile = this.resourceLoader.getResource(
            BASE_DIR + CLUSTER1_ID + FILE_DELIMITER + "setupfile"
        ).getFile().getAbsolutePath();

        final String configFile1 = this.resourceLoader.getResource(
            BASE_DIR + CLUSTER1_ID + FILE_DELIMITER + "config1"
        ).getFile().getAbsolutePath();
        final String configFile2 = this.resourceLoader.getResource(
            BASE_DIR + CLUSTER1_ID + FILE_DELIMITER + "config2"
        ).getFile().getAbsolutePath();
        final Set<String> configs = Sets.newHashSet(configFile1, configFile2);

        final String depFile1 = this.resourceLoader.getResource(
            BASE_DIR
                + CLUSTER1_ID
                + FILE_DELIMITER
                + "dep1"
        ).getFile().getAbsolutePath();
        final String depFile2 = this.resourceLoader.getResource(
            BASE_DIR
                + CLUSTER1_ID
                + FILE_DELIMITER
                + "dep2"
        ).getFile().getAbsolutePath();
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
        final String setUpFile = this.resourceLoader.getResource(
            BASE_DIR + CMD1_ID + FILE_DELIMITER + "setupfile"
        ).getFile().getAbsolutePath();

        final String configFile1 = this.resourceLoader.getResource(
            BASE_DIR + CMD1_ID + FILE_DELIMITER + "config1"
        ).getFile().getAbsolutePath();
        final String configFile2 = this.resourceLoader.getResource(
            BASE_DIR + CMD1_ID + FILE_DELIMITER + "config2"
        ).getFile().getAbsolutePath();
        final String configFile3 = this.resourceLoader.getResource(
            BASE_DIR + CMD1_ID + FILE_DELIMITER + GB18030_TXT
        ).getFile().getAbsolutePath();
        final Set<String> configs = Sets.newHashSet(configFile1, configFile2, configFile3);
        final String depFile1 = this.resourceLoader.getResource(
            BASE_DIR
                + CLUSTER1_ID
                + FILE_DELIMITER
                + "dep1"
        ).getFile().getAbsolutePath();
        final String depFile2 = this.resourceLoader.getResource(
            BASE_DIR
                + CLUSTER1_ID
                + FILE_DELIMITER
                + "dep2"
        ).getFile().getAbsolutePath();
        final Set<String> commandDependencies = Sets.newHashSet(depFile1, depFile2);

        final Set<String> tags = Sets.newHashSet(BASH_COMMAND_TAG);

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
            .withDependencies(commandDependencies)
            .withTags(tags)
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
}
