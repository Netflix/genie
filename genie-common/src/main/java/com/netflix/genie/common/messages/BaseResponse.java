/*
 *
 *  Copyright 2013 Netflix, Inc.
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

package com.netflix.genie.common.messages;

import java.io.Serializable;
import java.net.HttpURLConnection;

import javax.xml.bind.annotation.XmlElement;

import com.netflix.genie.common.exceptions.CloudServiceException;

/**
 * Base class to capture common response functionality.
 *
 * @author skrishnan
 */
public abstract class BaseResponse implements Serializable {

    private static final long serialVersionUID = -4736006120751273940L;

    /**
     * The HTTP error code for this response.
     */
    protected int errorCode = HttpURLConnection.HTTP_OK;

    /**
     * The human readable error message for the response.
     */
    protected String errorMsg = null;

    /**
     * Whether or not this response has an error.
     */
    protected boolean hasError = false;

    /**
     * Constructor.
     *
     * @param error
     *            initialize with this exception
     */
    public BaseResponse(CloudServiceException error) {
        this.errorCode = error.getErrorCode();
        this.errorMsg = error.getMessage();
        hasError = true;
    }

    /**
     * Constructor.
     */
    public BaseResponse() {
    }

    /**
     * Did the request succeed?
     *
     * @return true if request succeeded
     */
    public boolean requestSucceeded() {
        return !hasError;
    }

    /**
     * Did the request fail?
     *
     * @return true if the request has error
     */
    public boolean getHasError() {
        return hasError;
    }

    /**
     * Get the error code for this request.
     *
     * @return the error code for the response; zero if there was no error
     */
    public int getErrorCode() {
        return errorCode;
    }

    /**
     * Get the error message for this request.
     *
     * @return if the response failed, the error message from the
     *         {@code Exception}, otherwise null
     */
    @XmlElement(name = "errorMsg")
    public String getErrorMsg() {
        return errorMsg;
    }

    /**
     * Set the error message for this request.
     *
     * @param errorMsg
     *            the error message for this request
     */
    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }
}
