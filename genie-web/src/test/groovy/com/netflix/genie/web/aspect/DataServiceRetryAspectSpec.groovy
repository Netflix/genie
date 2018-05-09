package com.netflix.genie.web.aspect

import com.netflix.genie.common.exceptions.GenieException
import com.netflix.genie.common.exceptions.GenieServerException
import com.netflix.genie.common.internal.exceptions.unchecked.GenieIdAlreadyExistsException
import com.netflix.genie.common.internal.exceptions.unchecked.GenieRuntimeException
import com.netflix.genie.test.categories.UnitTest
import com.netflix.genie.web.jpa.services.JpaJobSearchServiceImpl
import com.netflix.genie.web.properties.DataServiceRetryProperties
import org.aspectj.lang.ProceedingJoinPoint
import org.junit.experimental.categories.Category
import org.springframework.aop.aspectj.annotation.AspectJProxyFactory
import org.springframework.dao.QueryTimeoutException
import spock.lang.Specification

/**
 * Unit tests for DataServiceRetryAspect
 *
 * @author amajumdar
 * @since 3.0.0
 */
@Category(UnitTest.class)
class DataServiceRetryAspectSpec extends Specification {
    def dataServiceRetryAspect

    def setup() {
        def dataServiceRetryProperties = new DataServiceRetryProperties()
        dataServiceRetryProperties.setNoOfRetries(2)
        dataServiceRetryProperties.setMaxInterval(10)
        dataServiceRetryProperties.setInitialInterval(10)
        dataServiceRetryAspect = new DataServiceRetryAspect(dataServiceRetryProperties);
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
        thrown(GenieServerException.class)
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
        thrown(GenieServerException.class)
        2 * joinPoint.proceed() >>
                { throw new QueryTimeoutException(null, null) } >>
                { throw new QueryTimeoutException(null, null) } >> null
    }

    def testDataServiceMethod() {
        given:
        def id = '1'
        def dataService = Mock(JpaJobSearchServiceImpl.class)
        AspectJProxyFactory factory = new AspectJProxyFactory(dataService)
        factory.addAspect(dataServiceRetryAspect)
        def dataServiceProxy = factory.getProxy()

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
        thrown(GenieServerException.class)
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
        thrown(GenieServerException.class)
        2 * dataService.getJob(id) >>
                { throw new QueryTimeoutException(null, null) } >>
                { throw new QueryTimeoutException(null, null) } >> null
    }
}
