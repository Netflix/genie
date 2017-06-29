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
import com.netflix.genie.common.dto.ClusterStatus;
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.dto.CommandStatus;
import com.netflix.genie.common.dto.Job;
import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.dto.JobStatus;
import org.apache.commons.lang3.ArrayUtils;
import org.springframework.hateoas.MediaTypes;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.restdocs.constraints.ConstraintDescriptions;
import org.springframework.restdocs.headers.HeaderDocumentation;
import org.springframework.restdocs.headers.RequestHeadersSnippet;
import org.springframework.restdocs.headers.ResponseHeadersSnippet;
import org.springframework.restdocs.hypermedia.HypermediaDocumentation;
import org.springframework.restdocs.hypermedia.LinksSnippet;
import org.springframework.restdocs.payload.FieldDescriptor;
import org.springframework.restdocs.payload.JsonFieldType;
import org.springframework.restdocs.payload.PayloadDocumentation;
import org.springframework.restdocs.payload.RequestFieldsSnippet;
import org.springframework.restdocs.payload.ResponseFieldsSnippet;
import org.springframework.restdocs.request.ParameterDescriptor;
import org.springframework.restdocs.request.PathParametersSnippet;
import org.springframework.restdocs.request.RequestDocumentation;
import org.springframework.restdocs.request.RequestParametersSnippet;
import org.springframework.restdocs.snippet.Attributes;
import org.springframework.util.StringUtils;

import java.util.Arrays;

/**
 * Helper class for getting field descriptors for various DTOs.
 *
 * @author tgianos
 * @since 3.0.0
 */
final class Snippets {
    static final RequestHeadersSnippet CONTENT_TYPE_HEADER = HeaderDocumentation.requestHeaders(
        HeaderDocumentation.headerWithName(HttpHeaders.CONTENT_TYPE).description(MediaType.APPLICATION_JSON_VALUE)
    );
    static final ResponseHeadersSnippet LOCATION_HEADER = HeaderDocumentation.responseHeaders(
        HeaderDocumentation.headerWithName(HttpHeaders.LOCATION).description("The URI")
    );
    static final ResponseHeadersSnippet HAL_CONTENT_TYPE_HEADER = HeaderDocumentation.responseHeaders(
        HeaderDocumentation.headerWithName(HttpHeaders.CONTENT_TYPE).description(MediaTypes.HAL_JSON_VALUE)
    );
    static final ResponseHeadersSnippet JSON_CONTENT_TYPE_HEADER = HeaderDocumentation.responseHeaders(
        HeaderDocumentation.headerWithName(HttpHeaders.CONTENT_TYPE).description(MediaType.APPLICATION_JSON_VALUE)
    );
    //    static final ResponseFieldsSnippet ERROR_FIELDS = PayloadDocumentation.responseFields(
//        PayloadDocumentation.fieldWithPath("error").description("The HTTP error that occurred, e.g. `Bad Request`"),
//        PayloadDocumentation.fieldWithPath("message").description("A description of the cause of the error"),
//        PayloadDocumentation.fieldWithPath("path").description("The path to which the request was made"),
//        PayloadDocumentation.fieldWithPath("status").description("The HTTP status code, e.g. `400`"),
//        PayloadDocumentation.fieldWithPath("timestamp")
//            .description("The time, in milliseconds, at which the error occurred")
//    );
    static final PathParametersSnippet ID_PATH_PARAM = RequestDocumentation.pathParameters(
        RequestDocumentation.parameterWithName("id").description("The resource id")
    );
    static final LinksSnippet SEARCH_LINKS = HypermediaDocumentation.links(
        HypermediaDocumentation
            .linkWithRel("self")
            .description("The current search"),
        HypermediaDocumentation
            .linkWithRel("first")
            .description("The first page for this search")
            .optional(),
        HypermediaDocumentation
            .linkWithRel("prev")
            .description("The previous page for this search")
            .optional(),
        HypermediaDocumentation
            .linkWithRel("next")
            .description("The next page for this search")
            .optional(),
        HypermediaDocumentation
            .linkWithRel("last")
            .description("The last page for this search")
            .optional()
    );
    static final LinksSnippet APPLICATION_LINKS = HypermediaDocumentation.links(
        HypermediaDocumentation
            .linkWithRel("self")
            .description("URI for this application"),
        HypermediaDocumentation
            .linkWithRel("commands")
            .description("Get all the commands using this application")
    );
    static final LinksSnippet CLUSTER_LINKS = HypermediaDocumentation.links(
        HypermediaDocumentation
            .linkWithRel("self")
            .description("URI for this cluster"),
        HypermediaDocumentation
            .linkWithRel("commands")
            .description("Get all the commands this cluster can use")
    );
    static final LinksSnippet COMMAND_LINKS = HypermediaDocumentation.links(
        HypermediaDocumentation
            .linkWithRel("self")
            .description("URI for this command"),
        HypermediaDocumentation
            .linkWithRel("applications")
            .description("Get all the applications this command depends on"),
        HypermediaDocumentation
            .linkWithRel("clusters")
            .description("Get all clusters this command is available on")
    );
    static final LinksSnippet JOB_REQUEST_LINKS = HypermediaDocumentation.links(
        HypermediaDocumentation
            .linkWithRel("self")
            .description("URI for this job request"),
        HypermediaDocumentation
            .linkWithRel("job")
            .description("The job that resulted from this request"),
        HypermediaDocumentation
            .linkWithRel("execution")
            .description("The job execution that resulted from this request"),
        HypermediaDocumentation
            .linkWithRel("output")
            .description("The output URI for the job"),
        HypermediaDocumentation
            .linkWithRel("status")
            .description("The current status of the job")
    );
    static final LinksSnippet JOB_LINKS = HypermediaDocumentation.links(
        HypermediaDocumentation
            .linkWithRel("self")
            .description("URI for this job"),
        HypermediaDocumentation
            .linkWithRel("request")
            .description("The request that kicked off this job"),
        HypermediaDocumentation
            .linkWithRel("execution")
            .description("The job execution information for this job"),
        HypermediaDocumentation
            .linkWithRel("output")
            .description("The output URI for the job"),
        HypermediaDocumentation
            .linkWithRel("status")
            .description("The current status of the job"),
        HypermediaDocumentation
            .linkWithRel("cluster")
            .description("The cluster this job ran on")
            .optional(),
        HypermediaDocumentation
            .linkWithRel("command")
            .description("The command this job executed")
            .optional(),
        HypermediaDocumentation
            .linkWithRel("applications")
            .description("The applications this job used")
            .optional()
    );
    static final LinksSnippet JOB_EXECUTION_LINKS = HypermediaDocumentation.links(
        HypermediaDocumentation
            .linkWithRel("self")
            .description("URI for this job execution"),
        HypermediaDocumentation
            .linkWithRel("job")
            .description("The job associated with this execution"),
        HypermediaDocumentation
            .linkWithRel("request")
            .description("The job request that spawned this execution"),
        HypermediaDocumentation
            .linkWithRel("output")
            .description("The output URI for the job"),
        HypermediaDocumentation
            .linkWithRel("status")
            .description("The current status of the job")
    );
    static final String CONSTRAINTS = "constraints";
    static final Attributes.Attribute EMPTY_CONSTRAINTS = Attributes.key(CONSTRAINTS).value("");
    static final FieldDescriptor[] CONFIG_FIELDS = new FieldDescriptor[]{
        PayloadDocumentation
            .fieldWithPath("[]")
            .description("Array of configuration file locations")
            .attributes(EMPTY_CONSTRAINTS),
    };
    static final FieldDescriptor[] DEPENDENCIES_FIELDS = new FieldDescriptor[]{
        PayloadDocumentation
            .fieldWithPath("[]")
            .description("Array of dependency file locations")
            .attributes(EMPTY_CONSTRAINTS),
    };
    static final FieldDescriptor[] TAGS_FIELDS = new FieldDescriptor[]{
        PayloadDocumentation
            .fieldWithPath("[]")
            .description("Array of tags")
            .attributes(EMPTY_CONSTRAINTS),
    };
    static final RequestFieldsSnippet PATCH_FIELDS = PayloadDocumentation.requestFields(
        PayloadDocumentation
            .fieldWithPath("[]")
            .description("Array of patches to apply")
            .attributes(EMPTY_CONSTRAINTS),
        PayloadDocumentation
            .fieldWithPath("[].op")
            .description("Patch operation to perform.")
            .attributes(Attributes.key(CONSTRAINTS).value("add, remove, replace, copy, move, test")),
        PayloadDocumentation
            .fieldWithPath("[].path")
            .description("The json path to operate on. e.g. /user")
            .attributes(EMPTY_CONSTRAINTS),
        PayloadDocumentation
            .fieldWithPath("[].from")
            .description("The json path to move or copy from. e.g. /user")
            .type(JsonFieldType.STRING)
            .attributes(EMPTY_CONSTRAINTS)
            .optional(),
        PayloadDocumentation
            .fieldWithPath("[].value")
            .description("The json value to put at the path for an add, replace or test")
            .type(JsonFieldType.STRING)
            .attributes(EMPTY_CONSTRAINTS)
            .optional()
    );
    static final RequestParametersSnippet APPLICATION_SEARCH_QUERY_PARAMETERS = RequestDocumentation.requestParameters(
        ArrayUtils.addAll(
            getCommonSearchParameters(),
            RequestDocumentation
                .parameterWithName("name")
                .description("The name of the applications to find. Use % to perform a regex like query")
                .optional(),
            RequestDocumentation
                .parameterWithName("user")
                .description("The user of the applications to find. Use % to perform a regex like query")
                .optional(),
            RequestDocumentation
                .parameterWithName("status")
                .description(
                    "The status(es) of the applications to find. Can have multiple. Options: "
                        + Arrays.toString(ApplicationStatus.values())
                )
                .optional(),
            RequestDocumentation
                .parameterWithName("tag")
                .description("The tag(s) of the applications to find. Can have multiple.")
                .optional(),
            RequestDocumentation
                .parameterWithName("type")
                .description("The type of the applications to find. Use % to perform a regex like query")
                .optional()
        )
    );
    static final RequestParametersSnippet CLUSTER_SEARCH_QUERY_PARAMETERS = RequestDocumentation.requestParameters(
        ArrayUtils.addAll(
            getCommonSearchParameters(),
            RequestDocumentation
                .parameterWithName("name")
                .description("The name of the clusters to find. Use % to perform a regex like query")
                .optional(),
            RequestDocumentation
                .parameterWithName("status")
                .description(
                    "The status(es) of the clusters to find. Can have multiple. Options: "
                        + Arrays.toString(ClusterStatus.values())
                )
                .optional(),
            RequestDocumentation
                .parameterWithName("tag")
                .description("The tag(s) of the clusters to find. Can have multiple.")
                .optional(),
            RequestDocumentation
                .parameterWithName("minUpdateTime")
                .description("The minimum time (in milliseconds from epoch UTC) that the cluster(s) were updated at.")
                .optional(),
            RequestDocumentation
                .parameterWithName("maxUpdateTime")
                .description("The maximum time (in milliseconds from epoch UTC) that the cluster(s) were updated at.")
                .optional()
        )
    );
    static final RequestParametersSnippet COMMAND_SEARCH_QUERY_PARAMETERS = RequestDocumentation.requestParameters(
        ArrayUtils.addAll(
            getCommonSearchParameters(),
            RequestDocumentation
                .parameterWithName("name")
                .description("The name of the commands to find. Use % to perform a regex like query")
                .optional(),
            RequestDocumentation
                .parameterWithName("user")
                .description("The user of the commands to find. Use % to perform a regex like query")
                .optional(),
            RequestDocumentation
                .parameterWithName("status")
                .description(
                    "The status(es) of the commands to find. Can have multiple. Options: "
                        + Arrays.toString(CommandStatus.values())
                )
                .optional(),
            RequestDocumentation
                .parameterWithName("tag")
                .description("The tag(s) of the commands to find. Can have multiple.")
                .optional()
        )
    );
    static final RequestParametersSnippet JOB_SEARCH_QUERY_PARAMETERS = RequestDocumentation.requestParameters(
        ArrayUtils.addAll(
            getCommonSearchParameters(),
            RequestDocumentation
                .parameterWithName("id")
                .description("The id of the jobs to find. Use % symbol for regex like search.")
                .optional(),
            RequestDocumentation
                .parameterWithName("name")
                .description("The name of the jobs to find. Use % symbol for regex like search.")
                .optional(),
            RequestDocumentation
                .parameterWithName("user")
                .description("The user of the jobs to find. Use % symbol for regex like search.")
                .optional(),
            RequestDocumentation
                .parameterWithName("status")
                .description(
                    "The status(es) of the jobs to find. Can have multiple. Options: "
                        + Arrays.toString(JobStatus.values())
                )
                .optional(),
            RequestDocumentation
                .parameterWithName("tag")
                .description("The tag(s) of the jobs to find. Can have multiple.")
                .optional(),
            RequestDocumentation
                .parameterWithName("clusterName")
                .description("The name of the cluster on which the jobs ran. Use % symbol for regex like search.")
                .optional(),
            RequestDocumentation
                .parameterWithName("clusterId")
                .description("The id of the cluster on which the jobs ran.")
                .optional(),
            RequestDocumentation
                .parameterWithName("commandName")
                .description(
                    "The name of the command which was executed by the job. Use % symbol for regex like search."
                )
                .optional(),
            RequestDocumentation
                .parameterWithName("commandId")
                .description("The id of the command which was executed by the job.")
                .optional(),
            RequestDocumentation
                .parameterWithName("minStarted")
                .description("The minimum started time of the job in milliseconds since epoch. (inclusive)")
                .optional(),
            RequestDocumentation
                .parameterWithName("maxStarted")
                .description("The maximum started time of the job in milliseconds since epoch. (exclusive)")
                .optional(),
            RequestDocumentation
                .parameterWithName("minFinished")
                .description("The minimum finished time of the job in milliseconds since epoch. (inclusive)")
                .optional(),
            RequestDocumentation
                .parameterWithName("maxFinished")
                .description("The maximum finished time of the job in milliseconds since epoch. (exclusive)")
                .optional()
        )
    );
    static final ResponseFieldsSnippet APPLICATION_SEARCH_RESULT_FIELDS = PayloadDocumentation
        .responseFields(
            ArrayUtils.addAll(
                getSearchResultFields(),
                PayloadDocumentation
                    .fieldWithPath("_embedded.applicationList")
                    .description("The found applications.")
                    .attributes(EMPTY_CONSTRAINTS)
            )
        );
    static final ResponseFieldsSnippet CLUSTER_SEARCH_RESULT_FIELDS = PayloadDocumentation
        .responseFields(
            ArrayUtils.addAll(
                getSearchResultFields(),
                PayloadDocumentation
                    .fieldWithPath("_embedded.clusterList")
                    .description("The found clusters.")
                    .attributes(EMPTY_CONSTRAINTS)
            )
        );
    static final ResponseFieldsSnippet COMMAND_SEARCH_RESULT_FIELDS = PayloadDocumentation
        .responseFields(
            ArrayUtils.addAll(
                getSearchResultFields(),
                PayloadDocumentation
                    .fieldWithPath("_embedded.commandList")
                    .description("The found commands.")
                    .attributes(EMPTY_CONSTRAINTS)
            )
        );
    static final ResponseFieldsSnippet JOB_SEARCH_RESULT_FIELDS = PayloadDocumentation
        .responseFields(
            ArrayUtils.addAll(
                getSearchResultFields(),
                PayloadDocumentation
                    .fieldWithPath("_embedded.jobSearchResultList")
                    .description("The found jobs.")
                    .attributes(EMPTY_CONSTRAINTS)
            )
        );
    static final ResponseFieldsSnippet OUTPUT_DIRECTORY_FIELDS = PayloadDocumentation
        .responseFields(
            PayloadDocumentation
                .fieldWithPath("parent")
                .description("Information about the parent of this directory")
                .attributes(EMPTY_CONSTRAINTS)
                .optional(),
            PayloadDocumentation
                .fieldWithPath("parent.name")
                .description("The name of the parent directory")
                .type(JsonFieldType.STRING)
                .attributes(EMPTY_CONSTRAINTS)
                .optional(),
            PayloadDocumentation
                .fieldWithPath("parent.url")
                .description("The url to get the parent")
                .type(JsonFieldType.STRING)
                .attributes(EMPTY_CONSTRAINTS)
                .optional(),
            PayloadDocumentation
                .fieldWithPath("parent.size")
                .description("The size of the parent in bytes")
                .type(JsonFieldType.NUMBER)
                .attributes(EMPTY_CONSTRAINTS)
                .optional(),
            PayloadDocumentation
                .fieldWithPath("parent.lastModified")
                .description("The last time the parent was modified in ISO8601 UTC with milliseconds included")
                .type(JsonFieldType.STRING)
                .attributes(EMPTY_CONSTRAINTS)
                .optional(),
            PayloadDocumentation
                .fieldWithPath("directories")
                .description("All the subdirectories of this directory")
                .attributes(EMPTY_CONSTRAINTS),
            PayloadDocumentation
                .fieldWithPath("directories[].name")
                .description("The name of the directory")
                .attributes(EMPTY_CONSTRAINTS),
            PayloadDocumentation
                .fieldWithPath("directories[].url")
                .description("The url to get the directory")
                .attributes(EMPTY_CONSTRAINTS),
            PayloadDocumentation
                .fieldWithPath("directories[].size")
                .description("The size of the directory in bytes")
                .attributes(EMPTY_CONSTRAINTS),
            PayloadDocumentation
                .fieldWithPath("directories[].lastModified")
                .description("The last time the directory was modified in ISO8601 UTC with milliseconds included")
                .attributes(EMPTY_CONSTRAINTS),
            PayloadDocumentation
                .fieldWithPath("files")
                .description("All the files in this directory")
                .attributes(EMPTY_CONSTRAINTS),
            PayloadDocumentation
                .fieldWithPath("files[].name")
                .description("The name of the file")
                .attributes(EMPTY_CONSTRAINTS),
            PayloadDocumentation
                .fieldWithPath("files[].url")
                .description("The url to get the file")
                .attributes(EMPTY_CONSTRAINTS),
            PayloadDocumentation
                .fieldWithPath("files[].size")
                .description("The size of the file in bytes")
                .attributes(EMPTY_CONSTRAINTS),
            PayloadDocumentation
                .fieldWithPath("files[].lastModified")
                .description("The last time the file was modified in ISO8601 UTC with milliseconds included")
                .attributes(EMPTY_CONSTRAINTS)
        );
    private static final ConstraintDescriptions COMMAND_CONSTRAINTS = new ConstraintDescriptions(Command.class);
    private static final ConstraintDescriptions JOB_REQUEST_CONSTRAINTS = new ConstraintDescriptions(JobRequest.class);
    private static final ConstraintDescriptions JOB_CONSTRAINTS = new ConstraintDescriptions(Job.class);
    private static final ConstraintDescriptions JOB_EXECUTION_CONSTRAINTS
        = new ConstraintDescriptions(JobExecution.class);
    private static final ConstraintDescriptions APPLICATION_CONSTRAINTS = new ConstraintDescriptions(Application.class);
    private static final ConstraintDescriptions CLUSTER_CONSTRAINTS = new ConstraintDescriptions(Cluster.class);

    private Snippets() {
    }

    static RequestFieldsSnippet getApplicationRequestPayload() {
        return PayloadDocumentation.requestFields(getApplicationFieldDescriptors());
    }

    static ResponseFieldsSnippet getApplicationResponsePayload() {
        return PayloadDocumentation.responseFields(getApplicationFieldDescriptors())
            .and(
                PayloadDocumentation
                    .fieldWithPath("_links")
                    .attributes(
                        Attributes
                            .key(CONSTRAINTS)
                            .value("")
                    )
                    .description("<<_hateoas,Links>> to other resources.")
                    .ignored()
            );
    }

    static RequestFieldsSnippet getClusterRequestPayload() {
        return PayloadDocumentation.requestFields(getClusterFieldDescriptors());
    }

    static ResponseFieldsSnippet getClusterResponsePayload() {
        return PayloadDocumentation.responseFields(getClusterFieldDescriptors())
            .and(
                PayloadDocumentation
                    .fieldWithPath("_links")
                    .attributes(
                        Attributes
                            .key(CONSTRAINTS)
                            .value("")
                    )
                    .description("<<_hateoas,Links>> to other resources.")
                    .ignored()
            );
    }

    static RequestFieldsSnippet getCommandRequestPayload() {
        return PayloadDocumentation.requestFields(getCommandFieldDescriptors());
    }

    static ResponseFieldsSnippet getCommandResponsePayload() {
        return PayloadDocumentation.responseFields(getCommandFieldDescriptors())
            .and(
                PayloadDocumentation
                    .fieldWithPath("_links")
                    .attributes(
                        Attributes
                            .key(CONSTRAINTS)
                            .value("")
                    )
                    .description("<<_hateoas,Links>> to other resources.")
                    .ignored()
            );
    }

    static RequestFieldsSnippet getJobRequestRequestPayload() {
        return PayloadDocumentation.requestFields(getJobRequestFieldDescriptors());
    }

    static ResponseFieldsSnippet getJobRequestResponsePayload() {
        return PayloadDocumentation.responseFields(getJobRequestFieldDescriptors())
            .and(
                PayloadDocumentation
                    .fieldWithPath("_links")
                    .attributes(
                        Attributes
                            .key(CONSTRAINTS)
                            .value("")
                    )
                    .description("<<_hateoas,Links>> to other resources.")
                    .ignored()
            );
    }

    static ResponseFieldsSnippet getJobResponsePayload() {
        return PayloadDocumentation.responseFields(getJobFieldDescriptors())
            .and(
                PayloadDocumentation
                    .fieldWithPath("_links")
                    .attributes(
                        Attributes
                            .key(CONSTRAINTS)
                            .value("")
                    )
                    .description("<<_hateoas,Links>> to other resources.")
                    .ignored()
            );
    }

    static ResponseFieldsSnippet getJobExecutionResponsePayload() {
        return PayloadDocumentation.responseFields(getJobExecutionFieldDescriptors())
            .and(
                PayloadDocumentation
                    .fieldWithPath("_links")
                    .attributes(
                        Attributes
                            .key(CONSTRAINTS)
                            .value("")
                    )
                    .description("<<_hateoas,Links>> to other resources.")
                    .ignored()
            );
    }

    private static FieldDescriptor[] getApplicationFieldDescriptors() {
        return ArrayUtils.addAll(
            getConfigFieldDescriptors(APPLICATION_CONSTRAINTS),
            PayloadDocumentation
                .fieldWithPath("type")
                .attributes(getConstraintsForField(APPLICATION_CONSTRAINTS, "type"))
                .description("The type of application this is (e.g. hadoop, presto, spark). Can be used to group.")
                .optional(),
            PayloadDocumentation
                .fieldWithPath("status")
                .attributes(getConstraintsForField(APPLICATION_CONSTRAINTS, "status"))
                .description(
                    "The status of the application. Options: " + Arrays.toString(ApplicationStatus.values())
                ),
            PayloadDocumentation
                .fieldWithPath("dependencies")
                .attributes(getConstraintsForField(APPLICATION_CONSTRAINTS, "dependencies"))
                .description("The dependencies for the application")
                .optional()
        );
    }

    private static FieldDescriptor[] getClusterFieldDescriptors() {
        return ArrayUtils.addAll(
            getConfigFieldDescriptors(CLUSTER_CONSTRAINTS),
            PayloadDocumentation
                .fieldWithPath("status")
                .attributes(getConstraintsForField(CLUSTER_CONSTRAINTS, "status"))
                .description(
                    "The status of the cluster. Options: " + Arrays.toString(ClusterStatus.values())
                ),
            PayloadDocumentation
                .fieldWithPath("dependencies")
                .attributes(getConstraintsForField(CLUSTER_CONSTRAINTS, "dependencies"))
                .description("The dependencies for the cluster")
                .optional()
        );
    }

    private static FieldDescriptor[] getCommandFieldDescriptors() {
        return ArrayUtils.addAll(
            getConfigFieldDescriptors(COMMAND_CONSTRAINTS),
            PayloadDocumentation
                .fieldWithPath("status")
                .attributes(getConstraintsForField(COMMAND_CONSTRAINTS, "status"))
                .description(
                    "The status of the command. Options: " + Arrays.toString(CommandStatus.values())
                ),
            PayloadDocumentation
                .fieldWithPath("executable")
                .attributes(getConstraintsForField(COMMAND_CONSTRAINTS, "executable"))
                .description("The executable to run on the Genie node when this command is used. e.g. /usr/bin/hadoop"),
            PayloadDocumentation
                .fieldWithPath("checkDelay")
                .attributes(getConstraintsForField(COMMAND_CONSTRAINTS, "checkDelay"))
                .description(
                    "The amount of time (in milliseconds) to delay between checks of the jobs using this command"
                ),
            PayloadDocumentation
                .fieldWithPath("memory")
                .attributes(getConstraintsForField(COMMAND_CONSTRAINTS, "memory"))
                .description(
                    "The default amount of memory (in MB) that should be allocated for instances of this command client"
                )
                .optional()
        );
    }

    private static FieldDescriptor[] getJobRequestFieldDescriptors() {
        return ArrayUtils.addAll(
            getSetupFieldDescriptors(JOB_REQUEST_CONSTRAINTS),
            PayloadDocumentation
                .fieldWithPath("commandArgs")
                .attributes(getConstraintsForField(JOB_REQUEST_CONSTRAINTS, "commandArgs"))
                .description("Any arguments to append to the command executable when the job is run"),
            PayloadDocumentation
                .fieldWithPath("clusterCriterias")
                .attributes(getConstraintsForField(JOB_REQUEST_CONSTRAINTS, "clusterCriterias"))
                .description(
                    "List of cluster criteria's for which a match will be attempted with register cluster tags."
                        + " Each set of tags within a given cluster criteria must have at least one non-blank "
                        + "(e.g. ' ', '    ', null) tag."
                ),
            PayloadDocumentation
                .fieldWithPath("commandCriteria")
                .attributes(getConstraintsForField(JOB_REQUEST_CONSTRAINTS, "commandCriteria"))
                .description(
                    "Set of tags which will attempt to match against the commands linked to selected cluster."
                     + " There must be at least one non-blank (e.g. ' ', '   ', null) criteria within the set"
                ),
            PayloadDocumentation
                .fieldWithPath("group")
                .attributes(getConstraintsForField(JOB_REQUEST_CONSTRAINTS, "group"))
                .description("A group that the job should be run under on the linux system")
                .optional(),
            PayloadDocumentation
                .fieldWithPath("disableLogArchival")
                .attributes(getConstraintsForField(JOB_REQUEST_CONSTRAINTS, "disableLogArchival"))
                .description("If you want to disable backing up job output files set this to true. Default: false")
                .optional(),
            PayloadDocumentation
                .fieldWithPath("email")
                .attributes(getConstraintsForField(JOB_REQUEST_CONSTRAINTS, "email"))
                .description("If you want e-mail notification on job completion enter address here")
                .optional(),
            PayloadDocumentation
                .fieldWithPath("cpu")
                .attributes(getConstraintsForField(JOB_REQUEST_CONSTRAINTS, "cpu"))
                .description("For future use. Currently has no impact.")
                .optional(),
            PayloadDocumentation
                .fieldWithPath("memory")
                .attributes(getConstraintsForField(JOB_REQUEST_CONSTRAINTS, "memory"))
                .description("The amount of memory (in MB) desired for job client. Cannot exceed configured max.")
                .optional(),
            PayloadDocumentation
                .fieldWithPath("timeout")
                .attributes(getConstraintsForField(JOB_REQUEST_CONSTRAINTS, "timeout"))
                .description(
                    "The timeout (in seconds) after which job will be killed by system, system setting used if not set"
                )
                .optional(),
            PayloadDocumentation
                .fieldWithPath("dependencies")
                .attributes(getConstraintsForField(JOB_REQUEST_CONSTRAINTS, "dependencies"))
                .description("URI's of dependency files which will be downloaded into job working directory at runtime")
                .optional(),
            PayloadDocumentation
                .fieldWithPath("applications")
                .attributes(getConstraintsForField(JOB_REQUEST_CONSTRAINTS, "applications"))
                .description(
                    "Complete list of application ids if power user wishes to override selected command defaults"
                )
                .optional()
        );
    }

    private static FieldDescriptor[] getJobFieldDescriptors() {
        return ArrayUtils.addAll(
            getCommonFieldDescriptors(JOB_CONSTRAINTS),
            PayloadDocumentation
                .fieldWithPath("commandArgs")
                .attributes(getConstraintsForField(JOB_CONSTRAINTS, "commandArgs"))
                .description("Any arguments to append to the command executable when the job is run"),
            PayloadDocumentation
                .fieldWithPath("status")
                .attributes(getConstraintsForField(JOB_CONSTRAINTS, "status"))
                .description("The status of the job. Options: " + Arrays.toString(JobStatus.values())),
            PayloadDocumentation
                .fieldWithPath("statusMsg")
                .attributes(getConstraintsForField(JOB_CONSTRAINTS, "statusMsg"))
                .description("The status message of the job")
                .optional(),
            PayloadDocumentation
                .fieldWithPath("started")
                .attributes(getConstraintsForField(JOB_CONSTRAINTS, "started"))
                .description("The time (UTC ISO8601 with millis) the job was started")
                .optional(),
            PayloadDocumentation
                .fieldWithPath("finished")
                .attributes(getConstraintsForField(JOB_CONSTRAINTS, "finished"))
                .description("The time (UTC ISO8601 with millis) the job finished")
                .optional(),
            PayloadDocumentation
                .fieldWithPath("archiveLocation")
                .attributes(getConstraintsForField(JOB_CONSTRAINTS, "archiveLocation"))
                .description("The URI where the working directory zip was stored")
                .optional(),
            PayloadDocumentation
                .fieldWithPath("clusterName")
                .attributes(getConstraintsForField(JOB_CONSTRAINTS, "clusterName"))
                .description("The name of the cluster the job was run on if it's been determined")
                .optional(),
            PayloadDocumentation
                .fieldWithPath("commandName")
                .attributes(getConstraintsForField(JOB_CONSTRAINTS, "commandName"))
                .description("The name of the command the job was run with if it's been determined")
                .optional(),
            PayloadDocumentation
                .fieldWithPath("runtime")
                .attributes(getConstraintsForField(JOB_CONSTRAINTS, "runtime"))
                .description("Runtime of the job in ISO8601 duration format")
                .optional()
        );
    }

    private static FieldDescriptor[] getJobExecutionFieldDescriptors() {
        return ArrayUtils.addAll(
            getBaseFieldDescriptors(JOB_EXECUTION_CONSTRAINTS),
            PayloadDocumentation
                .fieldWithPath("hostName")
                .attributes(getConstraintsForField(JOB_CONSTRAINTS, "hostName"))
                .description("The host name of the Genie node responsible for the job"),
            PayloadDocumentation
                .fieldWithPath("processId")
                .attributes(getConstraintsForField(JOB_CONSTRAINTS, "processId"))
                .description("The id of the job client process on the Genie node")
                .optional(),
            PayloadDocumentation
                .fieldWithPath("checkDelay")
                .attributes(getConstraintsForField(JOB_CONSTRAINTS, "checkDelay"))
                .description("The amount of time in milliseconds between checks of the job status by Genie")
                .optional(),
            PayloadDocumentation
                .fieldWithPath("timeout")
                .attributes(getConstraintsForField(JOB_CONSTRAINTS, "timeout"))
                .description("The date (UTC ISO8601 with millis) when the job will be killed by Genie due to timeout")
                .optional(),
            PayloadDocumentation
                .fieldWithPath("exitCode")
                .attributes(getConstraintsForField(JOB_CONSTRAINTS, "exitCode"))
                .description("The job client process exit code after the job is done")
                .optional(),
            PayloadDocumentation
                .fieldWithPath("memory")
                .attributes(getConstraintsForField(JOB_CONSTRAINTS, "memory"))
                .description("The amount of memory (in MB) allocated to the job client")
                .optional()
        );
    }

    private static FieldDescriptor[] getConfigFieldDescriptors(final ConstraintDescriptions constraintDescriptions) {
        return ArrayUtils.addAll(
            getSetupFieldDescriptors(constraintDescriptions),
            PayloadDocumentation
                .fieldWithPath("configs")
                .attributes(getConstraintsForField(constraintDescriptions, "configs"))
                .description("Any configuration files needed for the resource")
                .optional()
        );
    }

    private static FieldDescriptor[] getSetupFieldDescriptors(final ConstraintDescriptions constraintDescriptions) {
        return ArrayUtils.addAll(
            getCommonFieldDescriptors(constraintDescriptions),
            PayloadDocumentation
                .fieldWithPath("setupFile")
                .attributes(getConstraintsForField(constraintDescriptions, "setupFile"))
                .description("A location for any setup that needs to be done when installing")
                .optional()
        );
    }

    private static FieldDescriptor[] getCommonFieldDescriptors(final ConstraintDescriptions constraintDescriptions) {
        return ArrayUtils.addAll(
            getBaseFieldDescriptors(constraintDescriptions),
            PayloadDocumentation
                .fieldWithPath("name")
                .attributes(getConstraintsForField(constraintDescriptions, "name"))
                .description("The name"),
            PayloadDocumentation
                .fieldWithPath("user")
                .attributes(getConstraintsForField(constraintDescriptions, "user"))
                .description("The user"),
            PayloadDocumentation
                .fieldWithPath("version")
                .attributes(getConstraintsForField(constraintDescriptions, "version"))
                .description("The version"),
            PayloadDocumentation
                .fieldWithPath("description")
                .attributes(getConstraintsForField(constraintDescriptions, "description"))
                .description("Any description")
                .optional(),
            PayloadDocumentation
                .fieldWithPath("tags")
                .attributes(getConstraintsForField(constraintDescriptions, "tags"))
                .description("The tags")
                .optional()
        );
    }

    private static FieldDescriptor[] getBaseFieldDescriptors(final ConstraintDescriptions constraintDescriptions) {
        return new FieldDescriptor[]{
            PayloadDocumentation
                .fieldWithPath("id")
                .attributes(getConstraintsForField(constraintDescriptions, "id"))
                .description("The id. If not set the system will set one.")
                .optional(),
            PayloadDocumentation
                .fieldWithPath("created")
                .attributes(getConstraintsForField(constraintDescriptions, "created"))
                .description("The UTC time of creation. Set by system. ISO8601 format including milliseconds.")
                .optional(),
            PayloadDocumentation
                .fieldWithPath("updated")
                .attributes(getConstraintsForField(constraintDescriptions, "updated"))
                .description("The UTC time of last update. Set by system. ISO8601 format including milliseconds.")
                .optional(),
        };
    }

    private static FieldDescriptor[] getSearchResultFields() {
        return new FieldDescriptor[]{
            PayloadDocumentation
                .fieldWithPath("_links")
                .description("<<_hateoas,Links>> to other resources.")
                .attributes(EMPTY_CONSTRAINTS),
            PayloadDocumentation
                .fieldWithPath("page")
                .description("The result page information.")
                .attributes(EMPTY_CONSTRAINTS),
            PayloadDocumentation
                .fieldWithPath("page.size")
                .description("The number of elements in this page result.")
                .attributes(EMPTY_CONSTRAINTS),
            PayloadDocumentation
                .fieldWithPath("page.totalElements")
                .description("The total number of elements this search result could return.")
                .attributes(EMPTY_CONSTRAINTS),
            PayloadDocumentation
                .fieldWithPath("page.totalPages")
                .description("The total number of pages there could be at the current page size.")
                .attributes(EMPTY_CONSTRAINTS),
            PayloadDocumentation
                .fieldWithPath("page.number")
                .description("The current page number.")
                .attributes(EMPTY_CONSTRAINTS),
        };
    }

    private static ParameterDescriptor[] getCommonSearchParameters() {
        return new ParameterDescriptor[]{
            RequestDocumentation
                .parameterWithName("page")
                .description("The page number to get. Default to 0.")
                .optional(),
            RequestDocumentation
                .parameterWithName("size")
                .description("The size of the page to get. Default to 64.")
                .optional(),
            RequestDocumentation.parameterWithName("sort")
                .description("The fields to sort the results by. Defaults to 'updated,desc'.")
                .optional(),
        };
    }

    private static Attributes.Attribute getConstraintsForField(
        final ConstraintDescriptions constraints,
        final String fieldName
    ) {
        return Attributes
            .key(CONSTRAINTS)
            .value(StringUtils.collectionToDelimitedString(constraints.descriptionsForProperty(fieldName), ". "));
    }
}
