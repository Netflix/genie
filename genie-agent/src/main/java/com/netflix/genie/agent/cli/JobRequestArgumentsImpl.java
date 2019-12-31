/*
 *
 *  Copyright 2018 Netflix, Inc.
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
package com.netflix.genie.agent.cli;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.genie.common.external.dtos.v4.Criterion;
import com.netflix.genie.common.external.util.GenieObjectMapper;
import lombok.Getter;

import java.io.File;
import java.util.List;
import java.util.Set;

/**
 * Implementation of JobRequestArguments arguments delegate.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Getter
class JobRequestArgumentsImpl implements ArgumentDelegates.JobRequestArguments {

    @VisibleForTesting
    static final String DEFAULT_JOBS_DIRECTORY = "/tmp/genie/jobs/";

    private final MainCommandArguments mainCommandArguments;

    @Parameter(
        names = {"--jobDirectoryLocation", "--job-directory-location"},
        description = "The local directory in which the job directory is created and executed from",
        converter = ArgumentConverters.FileConverter.class
    )
    private File jobDirectoryLocation = new File(DEFAULT_JOBS_DIRECTORY);

    @Parameter(
        names = {"--interactive"},
        description = "Proxi standard {input,output,error} to the parent terminal, also disable console logging"
    )
    private boolean interactive;

    @Parameter(
        names = {"--archiveLocationPrefix", "--archive-location-prefix"},
        description = "Prefix for job folder archival location (Currently only s3 URIs are supported)",
        validateWith = ArgumentValidators.S3URIValidator.class
    )
    private String archiveLocationPrefix;

    @Parameter(
        names = {"--timeout"},
        description = "Time (in seconds) after which a running job is forcefully terminated"
    )
    private Integer timeout;

    @Parameter(
        names = {"--jobId", "--job-id"},
        description = "Unique job identifier"
    )
    private String jobId;

    @Parameter(
        names = {"--clusterCriterion", "--cluster-criterion"},
        description = "Criterion for cluster selection, can be repeated (see CRITERION SYNTAX)",
        converter = ArgumentConverters.CriterionConverter.class,
        splitter = NoopParameterSplitter.class
    )
    private List<Criterion> clusterCriteria = Lists.newArrayList();

    @Parameter(
        names = {"--commandCriterion", "--command-criterion"},
        description = "Criterion for command selection (see CRITERION SYNTAX)",
        converter = ArgumentConverters.CriterionConverter.class
    )
    private Criterion commandCriterion;

    @Parameter(
        names = {"--applicationIds", "--application-ids"},
        description = "Override the applications a command normally depends on, can be repeated"
    )
    private List<String> applicationIds = Lists.newArrayList();

    @Parameter(
        names = {"--jobName", "--job-name"},
        description = "Name of the job"
    )
    private String jobName;

    @Parameter(
        names = {"--user"},
        description = "Username launching this job",
        hidden = true // Not advertised to avoid abuse, but available for legitimate use cases
    )
    private String user = System.getProperty("user.name", "unknown-user");

    @Parameter(
        names = {"--email"},
        description = "Email address where to send a job completion notification"
    )
    private String email;

    @Parameter(
        names = {"--grouping"},
        description = "Group of jobs this job belongs to"
    )
    private String grouping;

    @Parameter(
        names = {"--groupingInstance", "--grouping-instance"},
        description = "Group instance this job belongs to"
    )
    private String groupingInstance;

    @Parameter(
        names = {"--jobDescription", "--job-description"},
        description = "Job description"
    )
    private String jobDescription;

    @Parameter(
        names = {"--jobTag", "--job-tag"},
        description = "Job tag, can be repeated",
        splitter = NoopParameterSplitter.class
    )
    private Set<String> jobTags = Sets.newHashSet();

    @Parameter(
        names = {"--jobVersion", "--job-version"},
        description = "Job version"
    )
    private String jobVersion;

    @Parameter(
        names = {"--jobMetadata", "--job-metadata"},
        description = "JSON job metadata",
        converter = ArgumentConverters.JSONConverter.class
    )
    private JsonNode jobMetadata = GenieObjectMapper.getMapper().createObjectNode();

    @Parameter(
        names = {"--apiJob", "--api-job"},
        description = "Whether the agent was launched by a Genie server in response to an API job submission",
        hidden = true // Do not expose this option via CLI to users
    )
    private boolean jobRequestedViaAPI;

    @Parameter(
        names = {"--jobConfiguration", "--job-configuration"},
        description = "URI or path of a job-level configuration file to attach, can be repeated",
        converter = ArgumentConverters.UriOrLocalPathConverter.class,
        validateValueWith = ArgumentValidators.URIListValidator.class,
        splitter = NoopParameterSplitter.class
    )
    private List<String> jobConfigurations = Lists.newArrayList();

    @Parameter(
        names = {"--jobDependency", "--job-dependency"},
        description = "URI or path of a job-level dependency file to attach, can be repeated",
        converter = ArgumentConverters.UriOrLocalPathConverter.class,
        validateValueWith = ArgumentValidators.URIListValidator.class,
        splitter = NoopParameterSplitter.class
    )
    private List<String> jobDependencies = Lists.newArrayList();

    @Parameter(
        names = {"--jobSetup", "--job-setup"},
        description = "URI or path of a job-level setup file to attach. The file is sourced during job setup",
        converter = ArgumentConverters.UriOrLocalPathConverter.class,
        validateValueWith = ArgumentValidators.URIValidator.class
    )
    private String jobSetup;

    JobRequestArgumentsImpl(final MainCommandArguments mainCommandArguments) {
        this.mainCommandArguments = mainCommandArguments;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> getCommandArguments() {
        return mainCommandArguments.get();
    }
}
