package com.netflix.genie.web.tasks.job

import com.netflix.genie.common.exceptions.GenieException
import spock.lang.Specification

/**
 * Unit tests for JobCompletionHandler
 *
 * @author amajumdar
 * @since 3.0.0
 */
class JobCompletionHandlerSpec extends Specification{
    JobCompletionService jobCompletionService = Mock(JobCompletionService)
    JobCompletionHandler jobCompletionHandler = new JobCompletionHandler(jobCompletionService)
    def testHandleJobCompletion(){
        when:
        jobCompletionHandler.handleJobCompletion(null)
        then:
        noExceptionThrown()
        1 * jobCompletionService.handleJobCompletion(null)
        when:
        jobCompletionHandler.handleJobCompletion(null)
        then:
        thrown(GenieException)
        1 * jobCompletionService.handleJobCompletion(null) >> { throw new GenieException(1,"")}
    }
}
