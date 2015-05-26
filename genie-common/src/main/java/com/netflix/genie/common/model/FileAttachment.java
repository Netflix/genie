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
package com.netflix.genie.common.model;

import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.wordnik.swagger.annotations.ApiModel;
import com.wordnik.swagger.annotations.ApiModelProperty;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.validator.constraints.NotBlank;
import org.hibernate.validator.constraints.NotEmpty;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Representation of a file attachment sent as part of the job request.
 *
 * @author skrishnan
 * @author tgianos
 */
@ApiModel(description = "An attachment for use with a job.")
public class FileAttachment implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Name of the file.
     */
    @ApiModelProperty(
            value = "The name of the file",
            required = true
    )
    @NotBlank(message = "No name entered for the attachment and is required.")
    private String name;

    /**
     * The data for the attachment.
     */
    @ApiModelProperty(
            value = "The bytes of the attachment",
            required = true
    )
    @NotEmpty(message = "No data entered for the attachment and is required")
    private byte[] data;

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
     * @param name name of the file for this attachment. Not null/empty/blank.
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    public void setName(final String name) throws GeniePreconditionException {
        if (StringUtils.isBlank(name)) {
            throw new GeniePreconditionException("No name entered for attachment. Unable to continue.");
        }
        this.name = name;
    }

    /**
     * Get the data for the attachment.
     *
     * @return the data for the attachment
     */
    public byte[] getData() {
        if (this.data != null) {
            return Arrays.copyOf(this.data, this.data.length);
        } else {
            return null;
        }
    }

    /**
     * Set the data for the attachment.
     *
     * @param data the data for the attachment. Not null or empty.
     * @throws GeniePreconditionException If preconditions aren't met.
     */
    public void setData(final byte[] data) throws GeniePreconditionException {
        if (data == null || data.length == 0) {
            throw new GeniePreconditionException("No data entered for attachment. Unable to continue.");
        }
        this.data = Arrays.copyOf(data, data.length);
    }
}
