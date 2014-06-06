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

package com.netflix.genie.common.model;

import java.io.Serializable;
import javax.activation.DataHandler;

/**
 * Representation of a file attachment sent as part of the job request.
 *
 * @author skrishnan
 */
public class FileAttachment implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Name of the file.
     */
    private String name;

    /**
     * The data for the attachment.
     */
    private transient DataHandler data;

    /**
     * Get the name of the file for this attachment.
     *
     * @return name of file for this attachment
     */
    public String getName() {
        return this.name;
    }

    /**
     * Set the name of the file for this attachment.
     *
     * @param name name of the file for this attachment
     */
    public void setName(final String name) {
        this.name = name;
    }

    /**
     * Get the data handler containing the data for the attachment.
     *
     * @return the data handler containing data for the attachment
     */
    public DataHandler getData() {
        return this.data;
    }

    /**
     * Set the data handler for the attachment.
     *
     * @param data the data handler for the attachment.
     */
    public void setData(final DataHandler data) {
        this.data = data;
    }
}
