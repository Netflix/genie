/*
 *
 *  Copyright 2016 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License")
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
package com.netflix.genie.web.tasks.job

import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableMap
import com.google.common.io.Files
import com.netflix.genie.common.dto.*
import com.netflix.genie.common.exceptions.GenieServerException
import com.netflix.genie.core.events.JobFinishedEvent
import com.netflix.genie.core.events.JobFinishedReason
import com.netflix.genie.core.properties.JobsProperties
import com.netflix.genie.core.services.JobPersistenceService
import com.netflix.genie.core.services.JobSearchService
import com.netflix.genie.core.services.MailService
import com.netflix.genie.core.services.impl.GenieFileTransferService
import com.netflix.genie.core.util.MetricsConstants
import com.netflix.genie.core.util.MetricsUtils
import com.netflix.genie.test.categories.UnitTest
import com.netflix.spectator.api.Counter
import com.netflix.spectator.api.Id
import com.netflix.spectator.api.Registry
import com.netflix.spectator.api.Timer
import org.assertj.core.util.Lists
import org.junit.Rule
import org.junit.experimental.categories.Category
import org.junit.rules.TemporaryFolder
import org.springframework.core.io.FileSystemResource
import org.springframework.retry.support.RetryTemplate
import spock.lang.Specification

import java.util.concurrent.TimeUnit

/**
 * Unit tests for JobCompletionHandler
 *
 * @author amajumdar
 * @since 3.0.0
 */
@Category(UnitTest.class)
class JobCompletionServiceSpec extends Specification {
    private static final String NAME = UUID.randomUUID().toString()
    private static final String USER = UUID.randomUUID().toString()
    private static final String VERSION = UUID.randomUUID().toString()
    private static final String CLUSTER_ID = UUID.randomUUID().toString()
    private static final String COMMAND_ID = UUID.randomUUID().toString()
    private static final long COMMAND_CHECK_DELAY = 1000
    private static final List<String> COMMAND_ARGS = Lists.newArrayList(UUID.randomUUID().toString())
    Cluster cluster;
    Command command;
    JobPersistenceService jobPersistenceService
    JobSearchService jobSearchService
    JobCompletionService jobCompletionService
    MailService mailService
    GenieFileTransferService genieFileTransferService
    JobsProperties jobsProperties
    Registry registry
    Id completionTimerId
    Timer completionTimer
    Map<String, String> timerTagsCapture
    Id errorCounterId
    Counter errorCounter
    List<Map<String, String>> counterTagsCaptures

    /**
     * Temporary folder used for storing fake job files. Deleted after tests are done.
     */
    @Rule
    public TemporaryFolder tmpJobDir = new TemporaryFolder()

    def setup() {
        jobPersistenceService = Mock(JobPersistenceService.class)
        jobSearchService = Mock(JobSearchService.class)
        mailService = Mock(MailService.class)
        genieFileTransferService = Mock(GenieFileTransferService.class)
        jobsProperties = new JobsProperties()
        counterTagsCaptures = new ArrayList<>()
        registry = Mock(Registry.class)
        errorCounter = Mock(Counter.class)
        errorCounterId = Mock(Id.class)
        completionTimerId = Mock(Id.class)
        completionTimerId.withTags(_) >> { args ->
            timerTagsCapture = args[0]
            return completionTimerId
        }
        errorCounterId.withTags(_) >> { args ->
            counterTagsCaptures.add(args[0])
            return errorCounterId
        }
        completionTimer = Mock(Timer.class)
        registry.createId("genie.jobs.completion.timer") >> completionTimerId
        registry.createId("genie.jobs.errors.count") >> errorCounterId
        registry.counter(errorCounterId) >> errorCounter
        registry.timer(completionTimerId) >> completionTimer
        jobsProperties.cleanup.deleteArchiveFile = false
        jobsProperties.cleanup.deleteDependencies = false
        jobsProperties.users.runAsUserEnabled = false
        jobCompletionService = new JobCompletionService(jobPersistenceService, jobSearchService,
                genieFileTransferService, new FileSystemResource("/tmp"), mailService, registry,
                jobsProperties, new RetryTemplate())
        cluster = new Cluster.Builder(NAME, USER, VERSION, ClusterStatus.UP)
                .withId(CLUSTER_ID)
                .build()
        command = new Command.Builder(
                NAME,
                USER,
                VERSION,
                CommandStatus.ACTIVE,
                null,
                COMMAND_CHECK_DELAY)
                .withId(COMMAND_ID)
                .build()
    }

    def handleJobCompletion() throws Exception {
        given:
        def jobId = "1"
        when:
        jobCompletionService.handleJobCompletion(new JobFinishedEvent(jobId, JobFinishedReason.KILLED, "null", this))
        then:
        noExceptionThrown()
        3 * jobSearchService.getJob(jobId) >>
                { throw new GenieServerException("null") } >>
                { throw new GenieServerException("null") } >>
                { throw new GenieServerException("null") }
        1 * completionTimer.record(_, TimeUnit.NANOSECONDS)
        verifyFailureTags(timerTagsCapture, new GenieServerException("null"), null, null)
        0 * errorCounter.increment()

        when:
        jobCompletionService.handleJobCompletion(new JobFinishedEvent(jobId, JobFinishedReason.KILLED, "null", this))
        then:
        noExceptionThrown()
        1 * jobSearchService.getJob(jobId) >> new Job.Builder(NAME, USER, VERSION)
                .withId(jobId).withStatus(JobStatus.SUCCEEDED).withCommandArgs(COMMAND_ARGS).build()
        0 * jobPersistenceService.updateJobStatus(jobId, _, _)
        verifySuccessTags(timerTagsCapture, null, null)
        0 * errorCounter.increment()

        when:
        jobCompletionService.handleJobCompletion(new JobFinishedEvent(jobId, JobFinishedReason.KILLED, "null", this))
        then:
        noExceptionThrown()
        1 * jobSearchService.getJob(jobId) >> new Job.Builder(NAME, USER, VERSION)
                .withId(jobId).withStatus(JobStatus.RUNNING).withCommandArgs(COMMAND_ARGS).build()
        1 * jobPersistenceService.updateJobStatus(jobId, _, _)
        1 * completionTimer.record(_, TimeUnit.NANOSECONDS)
        verifySuccessTags(timerTagsCapture, null, null)
        timerTagsCapture.get(JobCompletionService.JOB_FINAL_STATE).equals(JobStatus.FAILED.toString())
        4 * errorCounter.increment()
        counterTagsCaptures.containsAll(ImmutableList.of(
                ImmutableMap.of(
                        JobCompletionService.ERROR_SOURCE_TAG, "JOB_FINAL_UPDATE_FAILURE",
                        MetricsConstants.TagKeys.EXCEPTION_CLASS, FileNotFoundException.class.getCanonicalName()
                ),
                ImmutableMap.of(
                        JobCompletionService.ERROR_SOURCE_TAG, "JOB_PROCESS_CLEANUP_FAILURE",
                        MetricsConstants.TagKeys.EXCEPTION_CLASS, NullPointerException.class.canonicalName
                ),
                ImmutableMap.of(
                        JobCompletionService.ERROR_SOURCE_TAG, "JOB_DIRECTORY_FAILURE",
                        MetricsConstants.TagKeys.EXCEPTION_CLASS, NullPointerException.class.canonicalName

                ),
                ImmutableMap.of(
                        JobCompletionService.ERROR_SOURCE_TAG, "JOB_UPDATE_FAILURE",
                        MetricsConstants.TagKeys.EXCEPTION_CLASS, NullPointerException.class.canonicalName
                )
        ))

        when:
        jobCompletionService.handleJobCompletion(new JobFinishedEvent(jobId, JobFinishedReason.KILLED, "null", this))
        then:
        noExceptionThrown()
        1 * jobSearchService.getJob(jobId) >> new Job.Builder(NAME, USER, VERSION)
                .withId(jobId).withStatus(JobStatus.RUNNING).withCommandArgs(COMMAND_ARGS).build()
        1 * jobSearchService.getJobRequest(jobId) >> new JobRequest.Builder(NAME, USER, VERSION, null, null)
                .withId(jobId).withCommandArgs(COMMAND_ARGS).withEmail('admin@netflix.com').build()
        1 * jobPersistenceService.updateJobStatus(jobId, _, _)
        1 * mailService.sendEmail('admin@netflix.com', _, _)
        1 * completionTimer.record(_, TimeUnit.NANOSECONDS)
        verifySuccessTags(timerTagsCapture, null, null)
        timerTagsCapture.get(JobCompletionService.JOB_FINAL_STATE).equals(JobStatus.FAILED.toString())
        3 * errorCounter.increment()

        when:
        jobCompletionService.handleJobCompletion(new JobFinishedEvent(jobId, JobFinishedReason.PROCESS_COMPLETED, "null", this))
        then:
        1 * jobSearchService.getJob(jobId) >> new Job.Builder(NAME, USER, VERSION)
                .withId(jobId).withStatus(JobStatus.SUCCEEDED).withCommandArgs(COMMAND_ARGS).build()
        1 * jobSearchService.getJobCluster(jobId) >> cluster
        1 * jobSearchService.getJobCommand(jobId) >> command
        1 * completionTimer.record(_, TimeUnit.NANOSECONDS)
        verifySuccessTags(timerTagsCapture, cluster, command)

        when:
        jobCompletionService.handleJobCompletion(new JobFinishedEvent(jobId, JobFinishedReason.KILLED, "null", this))
        then:
        noExceptionThrown()
        3 * jobSearchService.getJob(jobId) >>
                { throw new GenieServerException("null") } >>
                { throw new GenieServerException("null") } >>
                { throw new GenieServerException("null") }
        1 * jobSearchService.getJobCluster(jobId) >> cluster
        1 * jobSearchService.getJobCommand(jobId) >> command
        1 * completionTimer.record(_, TimeUnit.NANOSECONDS)
        verifyFailureTags(timerTagsCapture, new GenieServerException("null"), cluster, command)

    }

    def deleteDependenciesDirectories() {
        given:
        def tempDirPath = tmpJobDir.getRoot().getAbsolutePath()
        def dependencyDirs = Arrays.asList(
                new File(tempDirPath + "/genie/applications/app1/dependencies"),
                new File(tempDirPath + "/genie/applications/app2/dependencies"),
                new File(tempDirPath + "/genie/cluster/cluster-x/dependencies"),
                new File(tempDirPath + "/genie/command/command-y/dependencies"),
        )
        dependencyDirs.forEach({ d -> Files.createParentDirs(new File(d, "a_dependency")) })
        def jobId = "1"
        def app1 = Mock(Application)
        app1.getId() >> new Optional<String>("app1")
        def app2 = Mock(Application)
        app2.getId() >> new Optional<String>("app2")
        jobSearchService.getJobApplications(jobId) >> Arrays.asList(app1, app2)
        def cluster = Mock(Cluster)
        cluster.getId() >> new Optional<String>("cluster-x")
        jobSearchService.getJobCluster(jobId) >> cluster
        def command = Mock(Command)
        command.getId() >> new Optional<String>("command-y")
        jobSearchService.getJobCommand(jobId) >> command

        when:
        jobCompletionService.deleteDependenciesDirectories(jobId, tmpJobDir.root)

        then:
        noExceptionThrown()
        dependencyDirs.forEach({ d ->
            assert !d.exists()
        })
    }

    /**
     * Verify the presence of failure tags
     *
     * @param tags tags to be verified
     * @param throwable exception thrown on failure
     * @return true if all failure tags exist
     */
    private void verifyFailureTags(
            final Map<String, String> tags,
            final Throwable throwable,
            final Cluster cluster,
            final Command command
    ) {
        assert tags.get(MetricsConstants.TagKeys.STATUS)
                .equals(MetricsConstants.TagValues.FAILURE)
        assert tags.get(MetricsConstants.TagKeys.EXCEPTION_CLASS)
                .equals(throwable.getClass().getCanonicalName())
        verifyCommonTags(tags, cluster, command);
    }

    /**
     * Verify the presence of success tags
     *
     * @param tags tags to be verified
     * @return true if all success tags exist
     */
    private void verifySuccessTags(
            final Map<String, String> tags,
            final Cluster cluster,
            final Command command
    ) {
        assert tags.get(MetricsConstants.TagKeys.STATUS).
                equals(MetricsConstants.TagValues.SUCCESS);
        verifyCommonTags(tags, cluster, command);
    }

    /**
     * Verify the presence of common tags
     *
     * @param tags tags to be verified
     * @return true if common tags exist
     */
    private void verifyCommonTags(
            final Map<String, String> tags,
            final Cluster cluster,
            final Command command
    ) {
        assert tags.get(MetricsConstants.TagKeys.CLUSTER_NAME)
                .equals(
                cluster != null
                        ? cluster.getName()
                        : MetricsConstants.TagValues.NO_CLUSTER_FOUND
        )

        assert tags.get(MetricsConstants.TagKeys.CLUSTER_ID)
                .equals(
                cluster != null
                        ? cluster.getId().orElse(MetricsConstants.TagValues.NO_ID_FOUND)
                        : MetricsConstants.TagValues.NO_CLUSTER_FOUND

        )

        assert tags.get(MetricsConstants.TagKeys.COMMAND_ID)
                .equals(
                command != null
                        ? command.getId().orElse(MetricsConstants.TagValues.NO_ID_FOUND)
                        : MetricsConstants.TagValues.NO_COMMAND_FOUND
        )

        assert tags.get(MetricsConstants.TagKeys.COMMAND_NAME)
                .equals(
                command != null
                        ? command.getName()
                        : MetricsConstants.TagValues.NO_COMMAND_FOUND
        )
    }

}
