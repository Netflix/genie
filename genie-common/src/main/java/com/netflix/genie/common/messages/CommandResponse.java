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
import com.netflix.genie.common.model.CommandElement;

/**
 * Represents response from the Commands REST resource.
 *
 * @author amsharma
 */
@XmlRootElement(name = "response")
public class CommandResponse extends BaseResponse {

    private static final long serialVersionUID = -1L;

    private String message;
    private CommandElement[] commands;

    /**
     * Constructor to be used if there is an error.
     *
     * @param error
     *            CloudServiceException for this response
     */
    public CommandResponse(CloudServiceException error) {
        super(error);
    }

    /**
     * Default constructor.
     */
    public CommandResponse() {
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
     * Get the array of commands for this response.
     *
     * @return array of commands
     */
    @XmlElementWrapper(name = "commands")
    @XmlElement(name = "command")
    public CommandElement[] getCommands() {
        if (commands == null) {
            return null;
        } else {
            return Arrays.copyOf(commands, commands.length);
        }
    }

    /**
     * Set the array of commands for this response.
     *
     * @param inCommands
     *            array of commands
     */
    public void setCommands(CommandElement[] inCommands) {
        if (inCommands == null) {
            this.commands = null;
        } else {
            this.commands = Arrays.copyOf(inCommands,
                    inCommands.length);
        }
    }
}
