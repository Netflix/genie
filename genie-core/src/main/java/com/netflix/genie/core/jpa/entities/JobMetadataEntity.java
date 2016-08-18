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

import com.netflix.genie.common.dto.JobMetadata;
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
import java.util.Optional;

/**
 * Entity representing any additional metadata associated with a job request gathered by the server.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Getter
@Setter
@Entity
@Table(name = "job_metadata")
public class JobMetadataEntity extends BaseEntity {
    private static final long serialVersionUID = -3800716806539057127L;

    @Basic
    @Column(name = "client_host")
    @Size(max = 255, message = "Max length in database is 255 characters")
    private String clientHost;

    @Basic
    @Column(name = "user_agent", length = 2048)
    @Size(max = 2048)
    private String userAgent;

    @Basic
    @Column(name = "num_attachments")
    @Min(value = 0, message = "Can't have less than zero attachments")
    private Integer numAttachments;

    @Basic
    @Column(name = "total_size_of_attachments")
    @Min(value = 0, message = "Can't have less than zero bytes total attachment size")
    private Long totalSizeOfAttachments;

    @Basic
    @Column(name = "std_out_size")
    @Min(value = 0, message = "Can't have less than zero bytes for std out size")
    private Long stdOutSize;

    @Basic
    @Column(name = "std_err_size")
    @Min(value = 0, message = "Can't have less than zero bytes for std err size")
    private Long stdErrSize;

    @OneToOne(optional = false, fetch = FetchType.LAZY)
    @JoinColumn(name = "id")
    @MapsId
    private JobRequestEntity request;

    /**
     * Get the client host.
     *
     * @return Optional of the client host
     */
    public Optional<String> getClientHost() {
        return Optional.ofNullable(this.clientHost);
    }

    /**
     * Get the user agent.
     *
     * @return Optional of the user agent
     */
    public Optional<String> getUserAgent() {
        return Optional.ofNullable(this.userAgent);
    }

    /**
     * Get the number of attachments.
     *
     * @return The number of attachments as an optional
     */
    public Optional<Integer> getNumAttachments() {
        return Optional.ofNullable(this.numAttachments);
    }

    /**
     * Get the total size of the attachments.
     *
     * @return The total size of attachments as an optional
     */
    public Optional<Long> getTotalSizeOfAttachments() {
        return Optional.ofNullable(this.totalSizeOfAttachments);
    }

    /**
     * Get the size of standard out for this job.
     *
     * @return The size (in bytes) of this jobs standard out file as Optional
     */
    public Optional<Long> getStdOutSize() {
        return Optional.ofNullable(this.stdOutSize);
    }

    /**
     * Get the size of standard error for this job.
     *
     * @return The size (in bytes) of this jobs standard error file as Optional
     */
    public Optional<Long> getStdErrSize() {
        return Optional.ofNullable(this.stdErrSize);
    }

    /**
     * Get a DTO representation of this entity.
     *
     * @return A JobMetadata instance containing copies of the data in this entity
     */
    public JobMetadata getDTO() {
        return new JobMetadata
            .Builder()
            .withId(this.getId())
            .withCreated(this.getCreated())
            .withUpdated(this.getUpdated())
            .withClientHost(this.clientHost)
            .withUserAgent(this.userAgent)
            .withNumAttachments(this.numAttachments)
            .withTotalSizeOfAttachments(this.totalSizeOfAttachments)
            .withStdOutSize(this.stdOutSize)
            .withStdErrSize(this.stdErrSize)
            .build();
    }
}
