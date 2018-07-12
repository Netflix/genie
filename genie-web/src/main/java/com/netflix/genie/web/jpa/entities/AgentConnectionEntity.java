/*
 *
 *  Copyright 2017 Netflix, Inc.
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
package com.netflix.genie.web.jpa.entities;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Table;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Size;

/**
 * Agent Connection Entity.
 *
 * @author mprimi
 * @since 4.0.0
 */
@NoArgsConstructor
@Getter
@Setter
@EqualsAndHashCode(callSuper = true, of = {"jobId", "serverHostname"})
@ToString(callSuper = true, of = {"jobId", "serverHostname"})
@Entity
@Table(name = "agent_connections")
public class AgentConnectionEntity extends AuditEntity {

    @Basic(optional = false)
    @Column(name = "job_id", nullable = false, unique = true, updatable = false)
    @NotBlank(message = "Must have a job id associated with this entity")
    @Size(max = 255, message = "Max length of id database is 255 characters")
    private String jobId;

    @Basic(optional = false)
    @Column(name = "server_hostname", nullable = false)
    @Size(max = 255, message = "Max length of id database is 255 characters")
    @NotBlank(message = "Must have a hostname associated with this entity")
    private String serverHostname;

    /**
     * Constructor.
     *
     * @param jobId          the job id
     * @param serverHostname the server with an active connection
     */
    public AgentConnectionEntity(
        final @NotBlank String jobId,
        final @NotBlank String serverHostname
    ) {
        super();
        this.jobId = jobId;
        this.serverHostname = serverHostname;
    }
}
