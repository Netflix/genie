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
package com.netflix.genie.agent.cli

import com.beust.jcommander.JCommander
import com.beust.jcommander.ParameterException
import com.beust.jcommander.ParametersDelegate
import com.netflix.genie.common.external.dtos.v4.Criterion
import com.netflix.genie.common.external.util.GenieObjectMapper
import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Files
import java.nio.file.Path

class JobRequestArgumentsImplSpec extends Specification {

    @TempDir
    Path temporaryFolder
    MainCommandArguments commandArguments
    TestOptions options
    JCommander jCommander

    void setup() {
        commandArguments = Mock(MainCommandArguments.class)
        options = new TestOptions(commandArguments)
        jCommander = new JCommander(options)
    }

    void cleanup() {
    }

    def "Defaults"() {
        when:
        jCommander.parse()

        then:
        options.jobRequestArguments.getJobDirectoryLocation() == new File(JobRequestArgumentsImpl.DEFAULT_JOBS_DIRECTORY)
        !options.jobRequestArguments.isInteractive()
        options.jobRequestArguments.getArchiveLocationPrefix() == null
        options.jobRequestArguments.getTimeout() == null
        options.jobRequestArguments.getJobId() == null
        options.jobRequestArguments.getClusterCriteria().isEmpty()
        options.jobRequestArguments.getCommandCriterion() == null
        options.jobRequestArguments.getApplicationIds().isEmpty()
        options.jobRequestArguments.getJobName() == null
        options.jobRequestArguments.getUser() == System.getProperty("user.name")
        options.jobRequestArguments.getEmail() == null
        options.jobRequestArguments.getGrouping() == null
        options.jobRequestArguments.getGroupingInstance() == null
        options.jobRequestArguments.getJobDescription() == null
        options.jobRequestArguments.getJobTags().isEmpty()
        options.jobRequestArguments.getJobVersion() == null
        options.jobRequestArguments.getJobMetadata() == GenieObjectMapper.getMapper().createObjectNode()
        !options.jobRequestArguments.isJobRequestedViaAPI()
        options.jobRequestArguments.getJobConfigurations().isEmpty()
        options.jobRequestArguments.getJobDependencies().isEmpty()
        options.jobRequestArguments.getJobSetup() == null
        !options.jobRequestArguments.isArchivingDisabled()

        when:
        options.jobRequestArguments.getCommandArguments()

        then:
        1 * commandArguments.get()
    }

    def "Parse"() {
        setup:
        def archiveLocationPrefix = "s3://bucket/" + UUID.randomUUID().toString()
        Path cfg1 = Files.createFile(temporaryFolder.resolve("cfg1.cfg"))
        Path cfg2 = Files.createFile(temporaryFolder.resolve("cfg2.cfg"))
        Path dep1 = Files.createFile(temporaryFolder.resolve("dep1.bin"))
        Path dep2 = Files.createFile(temporaryFolder.resolve("dep2.jar"))
        Path setup = Files.createFile(temporaryFolder.resolve("setup.sh"))

        when:
        jCommander.parse(
            "--job-directory-location", "/foo/bar",
            "--interactive",
            "--archive-location-prefix", archiveLocationPrefix,
            "--timeout", "10",
            "--job-id", "FooBar",
            "--cluster-criterion", "NAME=test",
            "--cluster-criterion", "NAME=prod",
            "--command-criterion", "STATUS=active",
            "--application-ids", "app1",
            "--application-ids", "app2",
            "--job-name", "n",
            "--email", "e",
            "--grouping", "g",
            "--grouping-instance", "gi",
            "--job-description", "jd",
            "--job-tag", "t1",
            "--job-tag", "t2",
            "--job-version", "1.0",
            "--job-metadata", "{\"foo\": false}",
            "--api-job",
            "--job-configuration", cfg1.toString(),
            "--job-configuration", cfg2.toString(),
            "--job-dependency", dep1.toString(),
            "--job-dependency", dep2.toString(),
            "--job-setup", setup.toString(),
            "--disable-archiving"
        )

        then:
        options.jobRequestArguments.getJobDirectoryLocation() == new File("/foo/bar")
        options.jobRequestArguments.isInteractive()
        options.jobRequestArguments.getArchiveLocationPrefix() == archiveLocationPrefix
        options.jobRequestArguments.getTimeout() == 10
        options.jobRequestArguments.getJobId() == "FooBar"
        options.jobRequestArguments.getClusterCriteria().size() == 2
        options.jobRequestArguments.getClusterCriteria().containsAll([
            new Criterion.Builder().withName("prod").build(),
            new Criterion.Builder().withName("test").build(),
        ])
        options.jobRequestArguments.getCommandCriterion() == new Criterion.Builder().withStatus("active").build()
        options.jobRequestArguments.getApplicationIds().size() == 2
        options.jobRequestArguments.getApplicationIds().containsAll(["app1", "app2"])
        options.jobRequestArguments.getJobName() == "n"
        options.jobRequestArguments.getUser() == System.getProperty("user.name")
        options.jobRequestArguments.getEmail() == "e"
        options.jobRequestArguments.getGrouping() == "g"
        options.jobRequestArguments.getGroupingInstance() == "gi"
        options.jobRequestArguments.getJobDescription() == "jd"
        options.jobRequestArguments.getJobTags().size() == 2
        options.jobRequestArguments.getJobTags().containsAll(["t1", "t2"])
        options.jobRequestArguments.getJobVersion() == "1.0"
        options.jobRequestArguments.getJobMetadata() == GenieObjectMapper.getMapper().createObjectNode().put("foo", false)
        options.jobRequestArguments.isJobRequestedViaAPI()
        options.jobRequestArguments.getJobConfigurations().containsAll([fileResource(cfg1), fileResource(cfg2)])
        options.jobRequestArguments.getJobDependencies().containsAll([fileResource(dep1), fileResource(dep2)])
        options.jobRequestArguments.getJobSetup() == fileResource(setup)
        options.jobRequestArguments.isArchivingDisabled()
    }

    static String fileResource(final Path file) {
        return "file:" + file.toAbsolutePath().toString()
    }

    def "Unknown parameters throw"() {

        when:
        jCommander.parse(
            "--interactive",
            "--job-id", "FooBar",
            "--cluster-criterion", "NAME=test",
            "--cluster-criterion", "NAME=prod",
            "--command-criterion", "STATUS=active",
            "--",
            "foo"
        )

        then:
        thrown(ParameterException)
    }

    def "Non S3 url throws ParameterException"() {

        when:
        jCommander.parse("--archive-location-prefix", "file://" + UUID.randomUUID().toString())

        then:
        thrown(ParameterException)

        when:
        jCommander.parse("--archive-location-prefix", UUID.randomUUID().toString())

        then:
        thrown(ParameterException)

        when:
        jCommander.parse("--archive-location-prefix", "")

        then:
        thrown(ParameterException)

    }

    def "Invalid file references throw ParameterException"() {

        setup:
        Path folder = Files.createDirectory(temporaryFolder.resolve(UUID.randomUUID().toString()))

        when:
        jCommander.parse("--job-dependency", folder.toString())

        then:
        thrown(ParameterException)

        when:
        jCommander.parse("--job-configuration", "/file/does/not/exist")

        then:
        thrown(ParameterException)
    }

    class TestOptions {

        @ParametersDelegate
        private final ArgumentDelegates.JobRequestArguments jobRequestArguments

        TestOptions(MainCommandArguments mainCommandArguments) {
            jobRequestArguments = new JobRequestArgumentsImpl(mainCommandArguments)
        }
    }
}
