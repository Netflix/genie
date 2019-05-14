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

import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.stereotype.Component;

/**
 * Application pointcut expressions.
 *
 * @author amajumdar
 * @since 3.0.0
 */
@Aspect
@Component
public class SystemArchitecture {
    /**
     * A join point is in the resource layer if the method is defined
     * in a type in the com.netflix.genie.web.controllers package or any sub-package
     * under that.
     */
    @Pointcut("within(com.netflix.genie.web.controllers..*)")
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
    @Pointcut("within(com.netflix.genie.web.data.services.jpa..*)")
    public void inDataLayer() {
    }

    /**
     * A resource service is the execution of any method defined on a controller.
     * This definition assumes that interfaces are placed in the
     * "resources" package, and that implementation types are in sub-packages.
     */
    @Pointcut("execution(* com.netflix.genie.web.controllers.*.*(..))")
    public void resourceOperation() {
    }

    /**
     * A service operation is the execution of any method defined on a
     * service class/interface. This definition assumes that interfaces are placed in the
     * "service" package, and that implementation types are in sub-packages.
     */
    @Pointcut("execution(* com.netflix.genie.web.services.*.*(..))")
    public void serviceOperation() {
    }

    /**
     * A data service operation is the execution of any method defined on a
     * dao interface. This definition assumes that interfaces are placed in the
     * "dao" package, and that implementation types are in sub-packages.
     */
    @Pointcut("execution(* com.netflix.genie.web.data.services.jpa.*.*(..))")
    public void dataOperation() {
    }
}
