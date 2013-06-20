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

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import com.netflix.genie.common.model.PigConfigElement;

/**
 * Represents request to the Pig Config REST resource.
 *
 * @author skrishnan
 */
@XmlRootElement(name = "request")
public class PigConfigRequest extends BaseRequest {

    private static final long serialVersionUID = -1L;

    private PigConfigElement pigConfig;

    /**
     * Constructor.
     */
    public PigConfigRequest() {
    }

    /**
     * Get the pig config for this request.
     *
     * @return the pigConfig element for this request
     */
    @XmlElement(name = "pigConfig")
    public PigConfigElement getPigConfig() {
        return pigConfig;
    }

    /**
     * Set the pig config for this request.
     *
     * @param pigConfig
     *            the pigConfig element for this request
     */
    public void setPigConfig(PigConfigElement pigConfig) {
        this.pigConfig = pigConfig;
    }
}
