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
package com.netflix.genie.web.tasks.job

import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableMap
import com.netflix.genie.common.dto.Job
import com.netflix.genie.common.dto.JobRequest
import com.netflix.genie.common.dto.JobStatus
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
import org.junit.experimental.categories.Category
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
class JobCompletionServiceSpec extends Specification{
    private static final String NAME = UUID.randomUUID().toString();
    private static final String USER = UUID.randomUUID().toString();
    private static final String VERSION = UUID.randomUUID().toString();
    private static final String COMMAND_ARGS = UUID.randomUUID().toString();
    JobPersistenceService jobPersistenceService;
    JobSearchService jobSearchService;
    JobCompletionService jobCompletionService;
    MailService mailService;
    GenieFileTransferService genieFileTransferService;
    JobsProperties jobsProperties;
    Registry registry;
    Id completionTimerId;
    Timer completionTimer;
    Map<String, String> timerTagsCapture;
    Id errorCounterId;
    Counter errorCounter;
    List<Map<String, String>> counterTagsCaptures;

    def setup(){
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
            return completionTimerId;
        }
        errorCounterId.withTags(_) >> { args ->
            counterTagsCaptures.add(args[0])
            return errorCounterId;
        }
        completionTimer = Mock(Timer.class)
        registry.createId("genie.jobs.completion.timer") >> completionTimerId
        registry.createId("genie.jobs.errors.count") >> errorCounterId
        registry.counter(errorCounterId) >> errorCounter
        registry.timer(completionTimerId) >> completionTimer
        jobsProperties.cleanup.deleteArchiveFile = false
        jobsProperties.cleanup.deleteDependencies = false
        jobsProperties.users.runAsUserEnabled = false
        jobCompletionService = new JobCompletionService( jobPersistenceService, jobSearchService,
                genieFileTransferService, new FileSystemResource("/tmp"), mailService, registry,
                jobsProperties, new RetryTemplate())
    }

    def handleJobCompletion() throws Exception{
        given:
        def jobId = "1"
        when:
        jobCompletionService.handleJobCompletion(new JobFinishedEvent(jobId, JobFinishedReason.KILLED, "null", this))
        then:
        noExceptionThrown()
        3 * jobSearchService.getJob(jobId) >>
                { throw new GenieServerException("null")} >>
                { throw new GenieServerException("null")} >>
                { throw new GenieServerException("null")}
        completionTimerId.withTags(MetricsUtils.newFailureTagsMapForException(new GenieServerException("null")))
        1 * completionTimer.record(_, TimeUnit.NANOSECONDS)
        timerTagsCapture == ImmutableMap.of(
                MetricsConstants.TagKeys.EXCEPTION_CLASS, GenieServerException.class.canonicalName,
                MetricsConstants.TagKeys.STATUS, MetricsConstants.TagValues.FAILURE,
        )
        0 * errorCounter.increment()

        when:
        jobCompletionService.handleJobCompletion(new JobFinishedEvent(jobId, JobFinishedReason.KILLED, "null", this))
        then:
        noExceptionThrown()
        1 * jobSearchService.getJob(jobId) >> new Job.Builder(NAME, USER, VERSION, COMMAND_ARGS)
                .withId(jobId).withStatus(JobStatus.SUCCEEDED).build();
        0 * jobPersistenceService.updateJobStatus(jobId,_,_)
        timerTagsCapture == ImmutableMap.of(
                MetricsConstants.TagKeys.STATUS, MetricsConstants.TagValues.SUCCESS,
        )
        0 * errorCounter.increment()

        when:
        jobCompletionService.handleJobCompletion(new JobFinishedEvent(jobId, JobFinishedReason.KILLED, "null", this))
        then:
        noExceptionThrown()
        1 * jobSearchService.getJob(jobId) >> new Job.Builder(NAME, USER, VERSION, COMMAND_ARGS)
                .withId(jobId).withStatus(JobStatus.RUNNING).build();
        1 * jobPersistenceService.updateJobStatus(jobId,_,_)
        1 * completionTimer.record(_, TimeUnit.NANOSECONDS)
        timerTagsCapture == ImmutableMap.of(
                MetricsConstants.TagKeys.STATUS, MetricsConstants.TagValues.SUCCESS,
        )
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
        1 * jobSearchService.getJob(jobId) >> new Job.Builder(NAME, USER, VERSION, COMMAND_ARGS)
                .withId(jobId).withStatus(JobStatus.RUNNING).build();
        1 * jobSearchService.getJobRequest(jobId) >> new JobRequest.Builder(NAME, USER, VERSION, COMMAND_ARGS, null, null)
                .withId(jobId).withEmail('admin@netflix.com').build();
        1 * jobPersistenceService.updateJobStatus(jobId,_,_)
        1 * mailService.sendEmail('admin@netflix.com',_,_)
        1 * completionTimer.record(_, TimeUnit.NANOSECONDS)
        timerTagsCapture == ImmutableMap.of(
                MetricsConstants.TagKeys.STATUS, MetricsConstants.TagValues.SUCCESS,
        )
        3 * errorCounter.increment()
    }
}
