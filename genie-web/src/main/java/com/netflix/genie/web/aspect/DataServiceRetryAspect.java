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
package com.netflix.genie.web.aspect;

import com.google.common.collect.ImmutableMap;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieServerException;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.Ordered;
import org.springframework.dao.CannotAcquireLockException;
import org.springframework.dao.ConcurrencyFailureException;
import org.springframework.dao.DeadlockLoserDataAccessException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.dao.PessimisticLockingFailureException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.retry.RetryListener;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

import javax.validation.ConstraintViolationException;

/**
 * Aspect implementation of retrying the data service methods on certain failures.
 * @author amajumdar
 * @since 3.0.0
 */
@Aspect
@Component
@Slf4j
public class DataServiceRetryAspect implements Ordered {
    private final RetryTemplate retryTemplate;

    /**
     * Constructor.
     * @param noOfRetries number of retries
     * @param initialInterval initial interval in milliseconds before retrying the first attempt
     * @param maxInterval maximum interval in milliseconds
     */
    @Autowired
    public DataServiceRetryAspect(
        @Value("${genie.data.service.retry.noOfRetries:5}") final int noOfRetries,
        @Value("${genie.data.service.retry.initialInterval:100}") final int initialInterval,
        @Value("${genie.data.service.retry.maxInterval:30000}") final int maxInterval) {
        retryTemplate = new RetryTemplate();
        retryTemplate.setRetryPolicy(new SimpleRetryPolicy(noOfRetries,
            new ImmutableMap.Builder<Class<? extends Throwable>, Boolean>()
                .put(CannotGetJdbcConnectionException.class, true)
                .put(CannotAcquireLockException.class, true)
                .put(DeadlockLoserDataAccessException.class, true)
                .put(OptimisticLockingFailureException.class, true)
                .put(PessimisticLockingFailureException.class, true)
                .put(ConcurrencyFailureException.class, true)
                .put(QueryTimeoutException.class, true)
                .put(TransientDataAccessResourceException.class, true)
                .build()));
        final ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
        backOffPolicy.setInitialInterval(initialInterval);
        backOffPolicy.setMaxInterval(maxInterval);
        retryTemplate.setBackOffPolicy(backOffPolicy);
    }

    /**
     * Sets the retry listeners for the retry template in use.
     * @param retryListeners retry listeners
     */
    public void setRetryListeners(final RetryListener[] retryListeners) {
        retryTemplate.setListeners(retryListeners);
    }

    /**
     * Aspect implementation method of retrying the data service method on certain failures.
     * @param pjp join point
     * @return return the data method response
     * @throws GenieException any exception thrown by the data service method
     */
    @Around("com.netflix.genie.web.aspect.SystemArchitecture.dataOperation()")
    public Object profile(final ProceedingJoinPoint pjp) throws GenieException {
        try {
            return retryTemplate.execute(context -> pjp.proceed());
        } catch (GenieException | ConstraintViolationException e) {
            throw e;
        } catch (Throwable e) {
            throw new GenieServerException(e);
        }
    }

    @Override
    public int getOrder() {
        return 0;
    }
}
