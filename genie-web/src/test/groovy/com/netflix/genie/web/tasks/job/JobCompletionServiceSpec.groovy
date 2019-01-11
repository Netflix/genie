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
import com.google.common.collect.ImmutableSet
import com.google.common.io.Files
import com.netflix.genie.common.dto.*
import com.netflix.genie.common.exceptions.GenieServerException
import com.netflix.genie.test.categories.UnitTest
import com.netflix.genie.web.events.JobFinishedEvent
import com.netflix.genie.web.events.JobFinishedReason
import com.netflix.genie.web.properties.JobsProperties
import com.netflix.genie.web.services.JobPersistenceService
import com.netflix.genie.web.services.JobSearchService
import com.netflix.genie.web.services.MailService
import com.netflix.genie.web.services.impl.GenieFileTransferService
import com.netflix.genie.web.util.MetricsConstants
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import org.assertj.core.util.Lists
import org.assertj.core.util.Sets
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
    private static final List<String> COMMAND_ARGS = Lists.newArrayList(UUID.randomUUID().toString())
    JobPersistenceService jobPersistenceService
    JobSearchService jobSearchService
    JobCompletionService jobCompletionService
    MailService mailService
    GenieFileTransferService genieFileTransferService
    JobsProperties jobsProperties
    MeterRegistry registry
    io.micrometer.core.instrument.Timer completionTimer
    Set<Tag> timerTagsCapture
    Counter errorCounter
    List<Set<Tag>> counterTagsCaptures

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
        jobsProperties = JobsProperties.getJobsPropertiesDefaults()
        counterTagsCaptures = new ArrayList<>()
        registry = Mock(MeterRegistry.class)
        errorCounter = Mock(Counter.class)
        completionTimer = Mock(io.micrometer.core.instrument.Timer.class)
        registry.timer(JobCompletionService.JOB_COMPLETION_TIMER_NAME, _ as Set<Tag>) >> { args ->
            timerTagsCapture = (Set<Tag>) args[1]
            return completionTimer
        }
        registry.counter(JobCompletionService.JOB_COMPLETION_ERROR_COUNTER_NAME, _ as Set<Tag>) >> { args ->
            counterTagsCaptures.add((Set<Tag>) args[1])
            return errorCounter
        }
        jobsProperties.cleanup.deleteArchiveFile = false
        jobsProperties.cleanup.deleteDependencies = false
        jobsProperties.users.runAsUserEnabled = false
        jobCompletionService = new JobCompletionService(jobPersistenceService, jobSearchService,
                genieFileTransferService, new FileSystemResource("/tmp"), mailService, registry,
                jobsProperties, new RetryTemplate())
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
        1 * completionTimer.record(_ as Long, TimeUnit.NANOSECONDS)
        timerTagsCapture == ImmutableSet.of(
                Tag.of(MetricsConstants.TagKeys.EXCEPTION_CLASS, GenieServerException.class.canonicalName),
                Tag.of(MetricsConstants.TagKeys.STATUS, MetricsConstants.TagValues.FAILURE)
        )
        0 * errorCounter.increment()

        when:
        jobCompletionService.handleJobCompletion(new JobFinishedEvent(jobId, JobFinishedReason.KILLED, "null", this))
        then:
        noExceptionThrown()
        1 * jobSearchService.getJob(jobId) >> new Job.Builder(NAME, USER, VERSION)
                .withId(jobId).withStatus(JobStatus.SUCCEEDED).withCommandArgs(COMMAND_ARGS).build()
        0 * jobPersistenceService.updateJobStatus(jobId, _ as JobStatus, _ as String)
        timerTagsCapture == ImmutableSet.of(
                Tag.of(MetricsConstants.TagKeys.STATUS, MetricsConstants.TagValues.SUCCESS)
        )
        0 * errorCounter.increment()

        when:
        jobCompletionService.handleJobCompletion(new JobFinishedEvent(jobId, JobFinishedReason.KILLED, "null", this))
        then:
        noExceptionThrown()
        1 * jobSearchService.getJob(jobId) >> new Job.Builder(NAME, USER, VERSION)
                .withId(jobId).withStatus(JobStatus.RUNNING).withCommandArgs(COMMAND_ARGS).build()
        1 * jobPersistenceService.updateJobStatus(jobId, _ as JobStatus, _ as String)
        1 * completionTimer.record(_ as Long, TimeUnit.NANOSECONDS)
        timerTagsCapture == ImmutableSet.of(
                Tag.of(MetricsConstants.TagKeys.STATUS, MetricsConstants.TagValues.SUCCESS),
                Tag.of(JobCompletionService.JOB_FINAL_STATE, JobStatus.FAILED.toString())
        )
        4 * errorCounter.increment()
        counterTagsCaptures.containsAll(ImmutableList.of(
                ImmutableSet.of(
                        Tag.of(JobCompletionService.ERROR_SOURCE_TAG, "JOB_FINAL_UPDATE_FAILURE"),
                        Tag.of(MetricsConstants.TagKeys.EXCEPTION_CLASS, FileNotFoundException.class.getCanonicalName())
                ),
                ImmutableSet.of(
                        Tag.of(JobCompletionService.ERROR_SOURCE_TAG, "JOB_PROCESS_CLEANUP_FAILURE"),
                        Tag.of(MetricsConstants.TagKeys.EXCEPTION_CLASS, NullPointerException.class.canonicalName)
                ),
                ImmutableSet.of(
                        Tag.of(JobCompletionService.ERROR_SOURCE_TAG, "JOB_DIRECTORY_FAILURE"),
                        Tag.of(MetricsConstants.TagKeys.EXCEPTION_CLASS, NullPointerException.class.canonicalName)

                ),
                ImmutableSet.of(
                        Tag.of(JobCompletionService.ERROR_SOURCE_TAG, "JOB_UPDATE_FAILURE"),
                        Tag.of(MetricsConstants.TagKeys.EXCEPTION_CLASS, NullPointerException.class.canonicalName)
                )
        ))

        when:
        jobCompletionService.handleJobCompletion(new JobFinishedEvent(jobId, JobFinishedReason.KILLED, "null", this))
        then:
        noExceptionThrown()
        1 * jobSearchService.getJob(jobId) >> new Job.Builder(NAME, USER, VERSION)
                .withId(jobId).withStatus(JobStatus.RUNNING).withCommandArgs(COMMAND_ARGS).build()
        1 * jobSearchService.getJobRequest(jobId) >> new JobRequest.Builder(NAME, USER, VERSION, Lists.newArrayList(), Sets.newHashSet())
                .withId(jobId).withCommandArgs(COMMAND_ARGS).withEmail('admin@netflix.com').build()
        1 * jobPersistenceService.updateJobStatus(jobId, _ as JobStatus, _ as String)
        1 * mailService.sendEmail('admin@netflix.com', _ as String, _ as String)
        1 * completionTimer.record(_ as Long, TimeUnit.NANOSECONDS)
        timerTagsCapture == ImmutableSet.of(
                Tag.of(MetricsConstants.TagKeys.STATUS, MetricsConstants.TagValues.SUCCESS),
                Tag.of(JobCompletionService.JOB_FINAL_STATE, JobStatus.FAILED.toString())
        )
        3 * errorCounter.increment()
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
        app1.getId() >> Optional.ofNullable("app1")
        def app2 = Mock(Application)
        app2.getId() >> Optional.ofNullable("app2")
        jobSearchService.getJobApplications(jobId) >> Arrays.asList(app1, app2)
        def cluster = Mock(Cluster)
        cluster.getId() >> Optional.ofNullable("cluster-x")
        jobSearchService.getJobCluster(jobId) >> cluster
        def command = Mock(Command)
        command.getId() >> Optional.ofNullable("command-y")
        jobSearchService.getJobCommand(jobId) >> command

        when:
        jobCompletionService.deleteDependenciesDirectories(jobId, tmpJobDir.root)

        then:
        noExceptionThrown()
        dependencyDirs.forEach({ d ->
            assert !d.exists()
        })
    }
}
