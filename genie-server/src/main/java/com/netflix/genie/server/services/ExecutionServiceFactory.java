/*
 *
 *  Copyright 2014 Netflix, Inc.
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
package com.netflix.genie.server.services;

import com.netflix.genie.common.exceptions.CloudServiceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory class to instantiate the ExecutionService.
 *
 * @author skrishnan
 */
//TODO: Get rid of this for some sort of DI framework
public final class ExecutionServiceFactory extends BaseServiceFactory {

    private static final Logger LOG = LoggerFactory
            .getLogger(ExecutionServiceFactory.class);

    // instances of the possible implementations
    private static volatile ExecutionService execService;

    /**
     * Get an instance of the configured execution service impl.
     *
     * @return singleton execution service impl
     * @throws CloudServiceException
     */
    public static synchronized ExecutionService getExecutionServiceImpl()
            throws CloudServiceException {
        if (execService == null) {
            LOG.info("Instantiating execution service impl");
            execService = (ExecutionService) instantiateFromProperty("netflix.genie.server.executionServiceImpl");
        }
        return execService;
    }
}
