/*
 *
 *  Copyright 2016 Netflix, Inc.
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
package com.netflix.genie.core.jpa.entities;

import com.netflix.genie.common.dto.JobRequestMetadata;
import lombok.Getter;
import lombok.Setter;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.JoinColumn;
import javax.persistence.MapsId;
import javax.persistence.OneToOne;
import javax.persistence.Table;
import javax.validation.constraints.Min;
import javax.validation.constraints.Size;

/**
 * Entity representing any additional metadata associated with a job request gathered by the server.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Entity
@Table(name = "job_request_metadata")
@Getter
@Setter
public class JobRequestMetadataEntity extends BaseEntity {
    private static final long serialVersionUID = -3800716806539057127L;

    @Basic
    @Column(name = "client_host")
    @Size(max = 255, message = "Max length in database is 255 characters")
    private String clientHost;

    @Basic
    @Column(name = "user_agent", length = 2048)
    @Size(max = 2048)
    private String userAgent;

    @Basic(optional = false)
    @Column(name = "num_attachments", nullable = false)
    @Min(value = 0, message = "Can't have less than zero attachments")
    private int numAttachments;

    @Basic(optional = false)
    @Column(name = "total_size_of_attachments", nullable = false)
    @Min(value = 0, message = "Can't have less than zero bytes total attachment size")
    private long totalSizeOfAttachments;

    @OneToOne(optional = false, fetch = FetchType.LAZY)
    @JoinColumn(name = "id")
    @MapsId
    private JobRequestEntity request;

    /**
     * Get a DTO representation of this entity.
     *
     * @return A JobRequestMetadata instance containing copies of the data in this entity
     */
    public JobRequestMetadata getDTO() {
        return new JobRequestMetadata
            .Builder()
            .withId(this.getId())
            .withCreated(this.getCreated())
            .withUpdated(this.getUpdated())
            .withClientHost(this.clientHost)
            .withUserAgent(this.userAgent)
            .withNumAttachments(this.numAttachments)
            .withTotalSizeOfAttachments(this.totalSizeOfAttachments)
            .build();
    }
}
