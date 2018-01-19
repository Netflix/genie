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

import com.netflix.genie.common.exceptions.GenieException
import com.netflix.genie.test.categories.UnitTest
import org.junit.experimental.categories.Category
import spock.lang.Specification

/**
 * Unit tests for JobCompletionHandler
 *
 * @author amajumdar
 * @since 3.0.0
 */
@Category(UnitTest.class)
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
