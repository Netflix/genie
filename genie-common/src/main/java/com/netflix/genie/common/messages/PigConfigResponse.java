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

import java.util.Arrays;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;

import com.netflix.genie.common.exceptions.CloudServiceException;
import com.netflix.genie.common.model.PigConfigElement;

/**
 * Represents response from the PigConfig REST resource.
 *
 * @author skrishnan
 */
@XmlRootElement(name = "response")
public class PigConfigResponse extends BaseResponse {

    private static final long serialVersionUID = -1L;

    private String message;
    private PigConfigElement[] pigConfigs;

    /**
     * Constructor to use if there is an error.
     *
     * @param error
     *            CloudServiceException for this response
     */
    public PigConfigResponse(CloudServiceException error) {
        super(error);
    }

    /**
     * Constructor.
     */
    public PigConfigResponse() {
    }

    /**
     * Human-readable message for this response.
     *
     * @return human-readable message for this response
     */
    @XmlElement(name = "message")
    public String getMessage() {
        return message;
    }

    /**
     * Sets the human-readable message for this response.
     *
     * @param message
     *            human-readable message for response
     */
    public void setMessage(String message) {
        this.message = message;
    }

    /**
     * Return an array of pig configs for this response.
     *
     * @return array of PigConfigs for response
     */
    @XmlElementWrapper(name = "pigConfigs")
    @XmlElement(name = "pigConfig")
    public PigConfigElement[] getPigConfigs() {
        if (pigConfigs == null) {
            return null;
        } else {
            return Arrays.copyOf(pigConfigs, pigConfigs.length);
        }
    }

    /**
     * Sets the pig configs for this response.
     *
     * @param inPigConfigs
     *            array of pig configs for this response
     */
    public void setPigConfigs(PigConfigElement[] inPigConfigs) {
        if (inPigConfigs == null) {
            this.pigConfigs = null;
        } else {
            this.pigConfigs = Arrays.copyOf(inPigConfigs, inPigConfigs.length);
        }
    }
}
