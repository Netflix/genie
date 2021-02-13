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
package com.netflix.genie.web.aspects

import com.netflix.genie.common.exceptions.GenieException
import com.netflix.genie.common.internal.exceptions.unchecked.GenieIdAlreadyExistsException
import com.netflix.genie.common.internal.exceptions.unchecked.GenieRuntimeException
import com.netflix.genie.web.data.services.PersistenceService
import com.netflix.genie.web.data.services.impl.jpa.JpaPersistenceServiceImpl
import com.netflix.genie.web.dtos.JobSubmission
import com.netflix.genie.web.exceptions.checked.IdAlreadyExistsException
import com.netflix.genie.web.properties.DataServiceRetryProperties
import org.aspectj.lang.ProceedingJoinPoint
import org.springframework.aop.aspectj.annotation.AspectJProxyFactory
import org.springframework.dao.QueryTimeoutException
import spock.lang.Specification

/**
 * Unit tests for {@link DataServiceRetryAspect}.
 *
 * @author amajumdar
 */
class DataServiceRetryAspectSpec extends Specification {
    DataServiceRetryAspect dataServiceRetryAspect

    def setup() {
        def dataServiceRetryProperties = new DataServiceRetryProperties()
        dataServiceRetryProperties.setNoOfRetries(2)
        dataServiceRetryProperties.setMaxInterval(10)
        dataServiceRetryProperties.setInitialInterval(10)
        dataServiceRetryAspect = new DataServiceRetryAspect(dataServiceRetryProperties)
    }

    def testProfile() {
        given:
        ProceedingJoinPoint joinPoint = Mock(ProceedingJoinPoint.class)

        when:
        dataServiceRetryAspect.profile(joinPoint)

        then:
        thrown(GenieException.class)
        1 * joinPoint.proceed() >> { throw new GenieException(1, "") }

        when:
        dataServiceRetryAspect.profile(joinPoint)

        then:
        thrown(GenieRuntimeException.class)
        2 * joinPoint.proceed() >> { throw new QueryTimeoutException(null, null) }

        when:
        dataServiceRetryAspect.profile(joinPoint)

        then:
        noExceptionThrown()
        1 * joinPoint.proceed() >> null

        when:
        dataServiceRetryAspect.profile(joinPoint)

        then:
        noExceptionThrown()
        2 * joinPoint.proceed() >> { throw new QueryTimeoutException(null, null) } >> null

        when:
        dataServiceRetryAspect.profile(joinPoint)

        then:
        thrown(GenieRuntimeException.class)
        2 * joinPoint.proceed() >>
            { throw new QueryTimeoutException(null, null) } >>
            { throw new QueryTimeoutException(null, null) } >> null
    }

    def testDataServiceMethod() {
        given:
        def id = '1'
        def dataService = Mock(JpaPersistenceServiceImpl.class)
        AspectJProxyFactory factory = new AspectJProxyFactory(dataService)
        factory.addAspect(dataServiceRetryAspect)
        PersistenceService dataServiceProxy = factory.getProxy()

        when:
        dataServiceProxy.getJob(id)

        then:
        thrown(GenieException.class)
        1 * dataService.getJob(id) >> { throw new GenieException(1, "") }

        when:
        dataServiceProxy.getJob(id)

        then:
        thrown(GenieRuntimeException.class)
        1 * dataService.getJob(id) >> { throw new GenieRuntimeException() }

        when:
        dataServiceProxy.getJob(id)

        then:
        thrown(GenieIdAlreadyExistsException.class)
        1 * dataService.getJob(id) >> { throw new GenieIdAlreadyExistsException() }

        when:
        dataServiceProxy.getJob(id)

        then:
        thrown(GenieRuntimeException.class)
        2 * dataService.getJob(id) >> { throw new QueryTimeoutException(null, null) }

        when:
        dataServiceProxy.getJob(id)

        then:
        noExceptionThrown()
        1 * dataService.getJob(id) >> null

        when:
        dataServiceProxy.getJob(id)

        then:
        noExceptionThrown()
        2 * dataService.getJob(id) >> { throw new QueryTimeoutException(null, null) } >> null

        when:
        dataServiceProxy.getJob(id)

        then:
        thrown(GenieRuntimeException.class)
        2 * dataService.getJob(id) >>
            { throw new QueryTimeoutException(null, null) } >>
            { throw new QueryTimeoutException(null, null) } >> null
    }

    def testSaveJobSubmission() {
        def jobSubmission = Mock(JobSubmission.class)
        def dataService = Mock(PersistenceService.class)
        AspectJProxyFactory factory = new AspectJProxyFactory(dataService)
        factory.addAspect(dataServiceRetryAspect)
        PersistenceService dataServiceProxy = factory.getProxy()

        when:
        dataServiceProxy.saveJobSubmission(jobSubmission)

        then:
        thrown(IdAlreadyExistsException.class)
        1 * dataService.saveJobSubmission(jobSubmission) >> { throw new IdAlreadyExistsException("conflict") }
    }
}
