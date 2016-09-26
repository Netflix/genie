package com.netflix.genie.client

import com.netflix.genie.common.model.Job
import com.netflix.genie.common.model.JobStatus
import spock.lang.Specification

import java.lang.reflect.Constructor
import java.util.concurrent.TimeUnit

/**
 * Created by ltudor on 12/5/15.
 */
class ExecutionServiceClientTest extends Specification {
    static Constructor ctr

    def setupSpec() {
        ctr = ExecutionServiceClient.class.getDeclaredConstructor()
        ctr.accessible = true
    }

    ExecutionServiceClient newInstance() {
        ctr.newInstance()
    }

    def "constructor creates timer"() {
        when:
        def x = newInstance()

        then:
        x.pollingTimer
    }

    def "close cancels timers and sets it to null"() {
        def timer = Mock(Timer)
        def x = newInstance()
        x.pollingTimer = timer

        when:
        x.close()

        then:
        1 * timer.purge()
        1 * timer.cancel()
        !x.pollingTimer
    }

    def "isFinished returns true only for SUCCEEDED, KILLED or FAILED"() {
        def x = newInstance()

        when:
        def r1 = x.isFinished(JobStatus.FAILED)
        def r2 = x.isFinished(JobStatus.INIT)
        def r3 = x.isFinished(JobStatus.KILLED)
        def r4 = x.isFinished(JobStatus.RUNNING)
        def r5 = x.isFinished(JobStatus.SUCCEEDED)
        def r6 = x.isFinished((JobStatus) null)

        then:
        r1
        !r2
        r3
        !r4
        r5
        !r6
    }

    def "isFinished(Job) returns isFinished(job.status)"() {
        def job = Stub(Job)
        job.status >>> [JobStatus.FAILED, JobStatus.INIT, JobStatus.KILLED, JobStatus.RUNNING, JobStatus.SUCCEEDED]
        def x = newInstance()

        when:
        def r1 = x.isFinished(job)
        def r2 = x.isFinished(job)
        def r3 = x.isFinished(job)
        def r4 = x.isFinished(job)
        def r5 = x.isFinished(job)

        then:
        r1
        !r2
        r3
        !r4
        r5
    }

    def "waitAndNotify submits taks to the timer"() {
        def job = Mock(Job)
        def notification = Mock(JobNotification)
        def timer = Mock(Timer)
        def x = newInstance()
        x.pollingTimer = timer

        when:
        x.waitAndNotify(job, notification)

        then:
        1 * timer.scheduleAtFixedRate(_, ExecutionServiceClient.DEFAULT_POLL_TIME, ExecutionServiceClient.DEFAULT_POLL_TIME)
    }

    def "waitAndNotify sends notification when job finished"() {
        def job = Stub(Job)
        job.status >>> [JobStatus.RUNNING, JobStatus.SUCCEEDED]
        def notification = Mock(JobNotification)
        def x = newInstance()

        when:
        x.waitAndNotify(job, notification)
        TimeUnit.SECONDS.sleep(5L)

        then:
        1 * notification.jobFinished(job)
    }
}

