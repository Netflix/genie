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
package com.netflix.genie.common.exceptions;

/**
 * The common exception class that represents a service failure. It includes an
 * HTTP error code and a human-readable error message.
 *
 * @author skrishnan
 * @author tgianos
 */
public class GenieException extends Exception {

    private static final long serialVersionUID = 1L;

    /* the HTTP error code */
    private final int errorCode;

    /**
     * Constructor.
     *
     * @param errorCode the HTTP status code for this exception
     * @param msg       human readable message
     * @param cause     reason for this exception
     */
    public GenieException(final int errorCode, final String msg, final Throwable cause) {
        super(msg, cause);
        this.errorCode = errorCode;
    }

    /**
     * Constructor.
     *
     * @param errorCode the HTTP status code for this exception
     * @param msg       human readable message
     */
    public GenieException(final int errorCode, final String msg) {
        super(msg);
        this.errorCode = errorCode;
    }

    /**
     * Return the HTTP status code for this exception.
     *
     * @return the HTTP status code
     */
    public int getErrorCode() {
        return errorCode;
    }
}
