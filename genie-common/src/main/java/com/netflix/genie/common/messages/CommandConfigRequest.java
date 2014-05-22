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

import com.netflix.genie.common.model.CommandConfig;


/**
 * Represents request to the Command's REST resource.
 *
 * @author amsharma
 */
@XmlRootElement(name = "request")
public class CommandConfigRequest extends BaseRequest {

    private static final long serialVersionUID = -1L;

    private CommandConfig commandConfig;

    /**
     * Constructor.
     */
    public CommandConfigRequest() {
    }

    /**
     * Gets the command for this request.
     *
     * @return command element for this request
     */
    @XmlElement(name = "commandConfig")
    public CommandConfig getCommandConfig() {
        return commandConfig;
    }

    /**
     * Sets the command element for this request.
     *
     * @param commandConfig
     *            command element for this request
     */
    public void setCommandConfig(CommandConfig commandConfig) {
        this.commandConfig = commandConfig;
    }
}
