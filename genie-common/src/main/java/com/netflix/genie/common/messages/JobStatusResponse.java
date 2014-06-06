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

package com.netflix.genie.common.messages;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import com.netflix.genie.common.exceptions.CloudServiceException;

/**
 * Represents a response from the Jobs REST resource, including only the status.
 *
 * @author skrishnan
 * @author bmundlapudi
 */
@XmlRootElement(name = "response")
public class JobStatusResponse extends BaseResponse {
    private static final long serialVersionUID = -1L;

    private String message;
    private String status;

    /**
     * Constructor to use if there is an error.
     *
     * @param error
     */
    public JobStatusResponse(CloudServiceException error) {
        super(error);
    }

    /**
     * Constructor.
     */
    public JobStatusResponse() {
    }

    /**
     * Get the human-readable message for this response.
     *
     * @return human-readable message
     */
    @XmlElement(name = "message")
    public String getMessage() {
        return message;
    }

    /**
     * Sets the human-readable message for this response.
     *
     * @param message
     *            human-readable message
     */
    public void setMessage(String message) {
        this.message = message;
    }

    /**
     * Get the status for this response.
     *
     * @return status string
     */
    @XmlElement(name = "status")
    public String getStatus() {
        return status;
    }

    /**
     * Set the status for this response.
     *
     * @param status
     *            status for this response
     */
    public void setStatus(String status) {
        this.status = status;
    }
}
