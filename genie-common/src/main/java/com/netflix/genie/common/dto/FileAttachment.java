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
package com.netflix.genie.common.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import org.hibernate.validator.constraints.NotBlank;
import org.hibernate.validator.constraints.NotEmpty;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Representation of a file attachment sent as part of the job request.
 *
 * @author skrishnan
 * @author tgianos
 * @since 1.0.0
 */
//TODO: There's gotta be a better way to deal with memory for this. Remove serializable once not used in Entities
@ApiModel(description = "An attachment for use with a job.")
public class FileAttachment implements Serializable {

    private static final long serialVersionUID = 1L;

    @ApiModelProperty(value = "The name of the file", required = true)
    @NotBlank(message = "No name entered for the attachment and is required.")
    private String name;

    @ApiModelProperty(value = "The date for the attachment as bytes", required = true)
    @NotEmpty(message = "No data entered for the attachment and is required")
    private byte[] data;

    /**
     * Create a new attachment.
     *
     * @param name The name of the attachment file.
     * @param data The data of the attachment as a byte array
     */
    @JsonCreator
    public FileAttachment(final String name, final byte[] data) {
        this.name = name;
        if (data != null) {
            this.data = Arrays.copyOf(data, data.length);
        }
    }

    /**
     * Get the name of the file for this attachment.
     *
     * @return name of file for this attachment
     */
    public String getName() {
        return this.name;
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
}
