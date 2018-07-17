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
package com.netflix.genie.client.exceptions;

import java.io.IOException;

/**
 * An exception class that represents all failures received by the client. It includes an
 * HTTP error code and a human-readable error message. The HTTP Error code is -1 in case the error is not
 * from the server.
 *
 * @author amsharma
 * @since 3.0.0
 */
public class GenieClientException extends IOException {

    /* the HTTP error code received from the server. -1 if the error is on the client side only.*/
    private final int errorCode;

    /**
     * Constructor.
     *
     * @param errorCode the HTTP status code for this exception
     * @param msg       human readable message
     */
    public GenieClientException(final int errorCode, final String msg) {
        super(errorCode + ": " + msg);
        this.errorCode = errorCode;
    }

    /**
     * Constructor.
     *
     * @param msg human readable message
     */
    public GenieClientException(final String msg) {
        super(msg);
        this.errorCode = -1;
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
