/*
 *
 *  Copyright 2015 Netflix, Inc.
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
package com.netflix.genie.server.resources;

import com.netflix.genie.common.exceptions.GenieException;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Exception mapper for CloudServiceExceptions.
 *
 * @author tgianos
 */
@Provider
public class GenieExceptionMapper implements ExceptionMapper<GenieException> {

    private static final Logger LOG = LoggerFactory.getLogger(GenieExceptionMapper.class);

    /**
     * Create a response object from the exception.
     *
     * @param ge The exception to create the HTTP response from
     * @return a Response object
     */
    @Override
    public Response toResponse(final GenieException ge) {
        final int code = ge.getErrorCode();
        final String errorMessage = ge.getLocalizedMessage();
        LOG.error("Error code: " + code + " Error Message: " + errorMessage, ge);
        return Response.status(code).entity(errorMessage).type(MediaType.TEXT_PLAIN).build();
    }
}
