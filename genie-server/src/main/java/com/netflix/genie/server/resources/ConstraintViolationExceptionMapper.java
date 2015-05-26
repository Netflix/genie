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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.ConstraintViolation;
import javax.validation.ConstraintViolationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;
import java.net.HttpURLConnection;

/**
 * Exception mapper for ConstraintViolationExceptions.
 *
 * @see ConstraintViolationException
 * @author tgianos
 */
@Provider
public class ConstraintViolationExceptionMapper implements ExceptionMapper<ConstraintViolationException> {

    private static final Logger LOG = LoggerFactory.getLogger(ConstraintViolationExceptionMapper.class);
    private static final String NEW_LINE = "\n";

    /**
     * Create a response object from the exception.
     *
     * @param cve The exception to create the HTTP response from
     * @return a Response object
     */
    @Override
    public Response toResponse(final ConstraintViolationException cve) {
        final int code = HttpURLConnection.HTTP_PRECON_FAILED;
        final StringBuilder builder = new StringBuilder();
        for (final ConstraintViolation<?> cv : cve.getConstraintViolations()) {
            if (builder.length() != 0) {
                builder.append(NEW_LINE);
            }
            builder.append(cv.getMessage());
        }
        final String errorMessage = builder.toString();
        LOG.error("Error code: " + code + " Error Message: " + errorMessage, cve);
        return Response.status(code).entity(errorMessage).type(MediaType.TEXT_PLAIN).build();
    }
}
