/*
 *
 *  Copyright 2026 Netflix, Inc.
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

import com.netflix.genie.web.apis.rest.v3.controllers.ApplicationRestController
import com.netflix.genie.web.data.services.impl.jpa.JpaPersistenceServiceImpl
import com.netflix.genie.web.data.services.impl.jpa.repositories.JpaJobRepository
import com.netflix.genie.web.services.JobLaunchService
import com.netflix.genie.web.services.impl.RequestForwardingServiceImpl
import org.springframework.aop.aspectj.AspectJExpressionPointcut
import spock.lang.Specification
import spock.lang.Unroll

/**
 * Tests that the named pointcuts declared by {@link SystemArchitecture} still resolve to the packages they claim to
 * target.
 * <p>
 * AspectJ silently matches nothing when a pointcut names a package or type that does not exist, so a package rename or
 * a typo degrades an aspect into dead code without any compile or startup failure. {@code dataOperation()} is already
 * covered indirectly by {@code DataServiceRetryAspectSpec}, which proxies a real bean. The remaining pointcuts
 * currently have no consumer, so nothing else would notice if they stopped matching.
 *
 * @since 4.0.0
 */
class SystemArchitectureSpec extends Specification {

    @Unroll
    def "#pointcut matches #target.simpleName: #shouldMatch"() {
        given:
        AspectJExpressionPointcut expression = new AspectJExpressionPointcut()
        expression.setExpression("com.netflix.genie.web.aspects.SystemArchitecture.${pointcut}()")

        expect: "the pointcut resolves and matches (or deliberately skips) the target type"
        target.getMethods().any { expression.matches(it, target) } == shouldMatch

        where:
        pointcut            | target                        || shouldMatch
        // within(..apis.rest.v3.controllers..*)
        "inResourceLayer"   | ApplicationRestController     || true
        "inResourceLayer"   | JobLaunchService              || false
        // execution(* ..apis.rest.v3.controllers.*.*(..))
        "resourceOperation" | ApplicationRestController     || true
        "resourceOperation" | JobLaunchService              || false
        // within(..services..*) - includes sub-packages
        "inServiceLayer"    | JobLaunchService              || true
        "inServiceLayer"    | RequestForwardingServiceImpl  || true
        // execution(* ..services.*.*(..)) - the package pattern is single level, but AspectJ matches a method
        // against every type that declares it, so an impl in a sub-package still matches through the interface
        // it implements in the services package
        "serviceOperation"  | JobLaunchService              || true
        "serviceOperation"  | RequestForwardingServiceImpl  || true
        // within(..data.services..*) - includes sub-packages
        "inDataLayer"       | JpaPersistenceServiceImpl     || true
        "inDataLayer"       | JpaJobRepository              || true
        // execution(* ..data.services.impl.jpa.*.*(..)) - single level only, so repositories are excluded
        "dataOperation"     | JpaPersistenceServiceImpl     || true
        "dataOperation"     | JpaJobRepository              || false
    }
}
