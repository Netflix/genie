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
package com.netflix.genie.web.aspects;

import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;

/**
 * Application pointcut expressions.
 *
 * @author amajumdar
 * @since 3.0.0
 */
@Aspect
public class SystemArchitecture {
    /**
     * A join point is in the resource layer if the method is defined
     * in a type in the com.netflix.genie.web.apis.rest.v3.controllers package or any sub-package
     * under that.
     */
    @Pointcut("within(com.netflix.genie.web.apis.rest.v3.controllers..*)")
    public void inResourceLayer() {
    }

    /**
     * A join point is in the service layer if the method is defined
     * in a type in the com.netflix.genie.web.services package or any sub-package
     * under that.
     */
    @Pointcut("within(com.netflix.genie.web.services..*)")
    public void inServiceLayer() {
    }

    /**
     * A join point is in the data service layer if the method is defined
     * in a type in the com.netflix.genie.web.data.services package or any sub-package
     * under that.
     */
    @Pointcut("within(com.netflix.genie.web.data.services..*)")
    public void inDataLayer() {
    }

    /**
     * A resource operation is the execution of any method declared on a type directly in the
     * com.netflix.genie.web.apis.rest.v3.controllers package. Types in sub-packages are not matched.
     */
    @Pointcut("execution(* com.netflix.genie.web.apis.rest.v3.controllers.*.*(..))")
    public void resourceOperation() {
    }

    /**
     * A service operation is the execution of any method declared on a type directly in the
     * com.netflix.genie.web.services package. Implementations in sub-packages are still matched when they
     * override a method declared on one of those types.
     */
    @Pointcut("execution(* com.netflix.genie.web.services.*.*(..))")
    public void serviceOperation() {
    }

    /**
     * A data service operation is the execution of any method declared on a type directly in the
     * com.netflix.genie.web.data.services.impl.jpa package, which in practice is
     * {@code JpaPersistenceServiceImpl}. Sub-packages such as the repositories and entities are not matched.
     */
    @Pointcut("execution(* com.netflix.genie.web.data.services.impl.jpa.*.*(..))")
    public void dataOperation() {
    }
}
