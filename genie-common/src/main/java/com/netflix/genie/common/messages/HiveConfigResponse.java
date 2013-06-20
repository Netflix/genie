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
import com.netflix.genie.common.model.HiveConfigElement;

/**
 * Represents response from the Hive Config REST resource.
 *
 * @author skrishnan
 */
@XmlRootElement(name = "response")
public class HiveConfigResponse extends BaseResponse {

    private static final long serialVersionUID = -1L;

    private String message;
    private HiveConfigElement[] hiveConfigs;

    /**
     * Constructor to be used if there is an error.
     *
     * @param error
     *            CloudServiceException for this response
     */
    public HiveConfigResponse(CloudServiceException error) {
        super(error);
    }

    /**
     * Default constructor.
     */
    public HiveConfigResponse() {
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
     * Set the human-readable message for this response.
     *
     * @param message
     *            human-readable message
     */
    public void setMessage(String message) {
        this.message = message;
    }

    /**
     * Get the array of hive configs for this response.
     *
     * @return array of hive configs
     */
    @XmlElementWrapper(name = "hiveConfigs")
    @XmlElement(name = "hiveConfig")
    public HiveConfigElement[] getHiveConfigs() {
        if (hiveConfigs == null) {
            return null;
        } else {
            return Arrays.copyOf(hiveConfigs, hiveConfigs.length);
        }
    }

    /**
     * Set the array of hive configs for this response.
     *
     * @param inHiveConfigs
     *            array of hive configs
     */
    public void setHiveConfigs(HiveConfigElement[] inHiveConfigs) {
        if (inHiveConfigs == null) {
            this.hiveConfigs = null;
        } else {
            this.hiveConfigs = Arrays.copyOf(inHiveConfigs,
                    inHiveConfigs.length);
        }
    }
}
