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
import com.google.common.collect.Lists
import com.google.common.collect.Sets
import com.google.common.io.Files
import com.netflix.genie.common.dto.Job
import com.netflix.genie.common.dto.JobExecution
import com.netflix.genie.common.dto.JobStatus
import com.netflix.genie.common.exceptions.GenieServerException
import com.netflix.genie.common.external.dtos.v4.Application
import com.netflix.genie.common.external.dtos.v4.Cluster
import com.netflix.genie.common.external.dtos.v4.Command
import com.netflix.genie.common.external.dtos.v4.JobMetadata
import com.netflix.genie.common.external.dtos.v4.JobRequest
import com.netflix.genie.common.internal.services.JobArchiveService
import com.netflix.genie.web.data.services.DataServices
import com.netflix.genie.web.data.services.PersistenceService
import com.netflix.genie.web.events.JobFinishedEvent
import com.netflix.genie.web.events.JobFinishedReason
import com.netflix.genie.web.properties.JobsProperties
import com.netflix.genie.web.services.MailService
import com.netflix.genie.web.util.MetricsConstants
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import org.junit.Rule
import org.junit.rules.TemporaryFolder
import org.springframework.core.io.FileSystemResource
import org.springframework.retry.support.RetryTemplate
import spock.lang.Specification

import java.util.concurrent.TimeUnit

/**
 * Unit tests for {@link JobCompletionService}.
 *
 * @author amajumdar
 */
@SuppressWarnings("GroovyAccessibility")
class JobCompletionServiceSpec extends Specification {
    private static final String NAME = UUID.randomUUID().toString()
    private static final String USER = UUID.randomUUID().toString()
    private static final String VERSION = UUID.randomUUID().toString()
    private static final List<String> COMMAND_ARGS = Lists.newArrayList(UUID.randomUUID().toString())
    PersistenceService persistenceService
    JobCompletionService jobCompletionService
    MailService mailService
    JobArchiveService jobArchiveService
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
        persistenceService = Mock(PersistenceService.class)
        mailService = Mock(MailService.class)
        jobArchiveService = Mock(JobArchiveService.class)
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
        jobsProperties.cleanup.deleteDependencies = false
        jobsProperties.users.runAsUserEnabled = false
        def dataServices = Mock(DataServices) {
            getPersistenceService() >> this.persistenceService
        }
        jobCompletionService = new JobCompletionService(
            dataServices,
            jobArchiveService,
            new FileSystemResource(this.tmpJobDir.getRoot()),
            mailService,
            registry,
            jobsProperties,
            new RetryTemplate()
        )
    }

    def handleJobCompletion() throws Exception {
        given:
        def jobId = "1"
        def jobRequest = Mock(JobRequest)
        def jobMetadata = Mock(JobMetadata)

        when:
        jobCompletionService.handleJobCompletion(new JobFinishedEvent(jobId, JobFinishedReason.KILLED, "null", this))

        then:
        noExceptionThrown()
        3 * persistenceService.getJob(jobId) >>
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
        1 * persistenceService.getJob(jobId) >> new Job.Builder(NAME, USER, VERSION)
            .withId(jobId).withStatus(JobStatus.SUCCEEDED).withCommandArgs(COMMAND_ARGS).build()
        0 * persistenceService.updateJobStatus(jobId, _ as JobStatus, _ as String)
        timerTagsCapture == ImmutableSet.of(
            Tag.of(MetricsConstants.TagKeys.STATUS, MetricsConstants.TagValues.SUCCESS)
        )
        0 * errorCounter.increment()

        when:
        jobCompletionService.handleJobCompletion(new JobFinishedEvent(jobId, JobFinishedReason.KILLED, "null", this))
        then:
        noExceptionThrown()
        1 * persistenceService.getJob(jobId) >> new Job.Builder(NAME, USER, VERSION)
            .withId(jobId).withStatus(JobStatus.RUNNING).withCommandArgs(COMMAND_ARGS).build()
        1 * persistenceService.updateJobStatus(jobId, _ as JobStatus, _ as String)
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
        def jobDir = this.tmpJobDir.newFolder(jobId)
        def genieDir = java.nio.file.Files.createDirectory(jobDir.toPath().resolve("genie"))
        def doneFile = genieDir.resolve("genie.done")
        java.nio.file.Files.write(doneFile, Lists.newArrayList("{", "\"exitCode\":999", "}"))
        def killReasonFile = genieDir.resolve("kill-reason")
        java.nio.file.Files.write(killReasonFile, Lists.newArrayList("{", "\"killReason\":\"blah\"", "}"))
        jobCompletionService.handleJobCompletion(new JobFinishedEvent(jobId, JobFinishedReason.KILLED, "null", this))

        then:
        noExceptionThrown()
        1 * persistenceService.getJob(jobId) >> new Job.Builder(NAME, USER, VERSION)
            .withId(jobId)
            .withStatus(JobStatus.RUNNING)
            .withCommandArgs(COMMAND_ARGS)
            .build()
        3 * persistenceService.getJobStatus(jobId) >> com.netflix.genie.common.external.dtos.v4.JobStatus.RUNNING
        1 * persistenceService.getJobExecution(jobId) >> Mock(JobExecution) {
            getProcessId() >> Optional.empty()
        }
        1 * persistenceService.setJobCompletionInformation(
            jobId,
            999,
            JobStatus.KILLED,
            "blah",
            null,
            null
        )
        1 * persistenceService.getJobRequest(jobId) >> jobRequest
        1 * jobRequest.getMetadata() >> jobMetadata
        2 * jobMetadata.getName() >> NAME
        1 * jobMetadata.getUser() >> USER
        1 * jobMetadata.getEmail() >> Optional.of("admin@genie.com")
        1 * jobMetadata.getTags() >> Sets.newHashSet()
        1 * jobMetadata.getDescription() >> Optional.empty()
        1 * mailService.sendEmail('admin@genie.com', _ as String, _ as String)
        1 * completionTimer.record(_ as Long, TimeUnit.NANOSECONDS)
        timerTagsCapture == ImmutableSet.of(
            Tag.of(MetricsConstants.TagKeys.STATUS, MetricsConstants.TagValues.SUCCESS),
            Tag.of(JobCompletionService.JOB_FINAL_STATE, JobStatus.FAILED.toString())
        )
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
        app1.getId() >> "app1"
        def app2 = Mock(Application)
        app2.getId() >> "app2"
        persistenceService.getJobApplications(jobId) >> Lists.newArrayList(app1, app2)
        def cluster = Mock(Cluster)
        cluster.getId() >> "cluster-x"
        persistenceService.getJobCluster(jobId) >> cluster
        def command = Mock(Command)
        command.getId() >> "command-y"
        persistenceService.getJobCommand(jobId) >> command

        when:
        jobCompletionService.deleteDependenciesDirectories(jobId, tmpJobDir.root)

        then:
        noExceptionThrown()
        dependencyDirs.forEach({ d ->
            assert !d.exists()
        })
    }
}
