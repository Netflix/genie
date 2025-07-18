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
package com.netflix.genie.web.data.services.impl.jpa.entities;

import com.fasterxml.jackson.databind.JsonNode;
import com.netflix.genie.web.data.services.impl.jpa.converters.IntegerToLongConverter;
import com.netflix.genie.web.data.services.impl.jpa.converters.JsonAttributeConverter;
import com.netflix.genie.web.data.services.impl.jpa.listeners.JobEntityListener;
import com.netflix.genie.web.data.services.impl.jpa.queries.predicates.PredicateUtils;
import com.netflix.genie.web.data.services.impl.jpa.queries.projections.JobApiProjection;
import com.netflix.genie.web.data.services.impl.jpa.queries.projections.JobApplicationsProjection;
import com.netflix.genie.web.data.services.impl.jpa.queries.projections.JobArchiveLocationProjection;
import com.netflix.genie.web.data.services.impl.jpa.queries.projections.JobClusterProjection;
import com.netflix.genie.web.data.services.impl.jpa.queries.projections.JobCommandProjection;
import com.netflix.genie.web.data.services.impl.jpa.queries.projections.JobExecutionProjection;
import com.netflix.genie.web.data.services.impl.jpa.queries.projections.JobMetadataProjection;
import com.netflix.genie.web.data.services.impl.jpa.queries.projections.JobProjection;
import com.netflix.genie.web.data.services.impl.jpa.queries.projections.JobSearchProjection;
import com.netflix.genie.web.data.services.impl.jpa.queries.projections.StatusProjection;
import com.netflix.genie.web.data.services.impl.jpa.queries.projections.v4.FinishedJobProjection;
import com.netflix.genie.web.data.services.impl.jpa.queries.projections.v4.JobRequestProjection;
import com.netflix.genie.web.data.services.impl.jpa.queries.projections.v4.JobSpecificationProjection;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import jakarta.annotation.Nullable;
import jakarta.persistence.Basic;
import jakarta.persistence.CascadeType;
import jakarta.persistence.CollectionTable;
import jakarta.persistence.Column;
import jakarta.persistence.Convert;
import jakarta.persistence.ElementCollection;
import jakarta.persistence.Entity;
import jakarta.persistence.EntityListeners;
import jakarta.persistence.FetchType;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.JoinTable;
import jakarta.persistence.Lob;
import jakarta.persistence.ManyToMany;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.MapKeyColumn;
import jakarta.persistence.NamedAttributeNode;
import jakarta.persistence.NamedEntityGraph;
import jakarta.persistence.NamedEntityGraphs;
import jakarta.persistence.NamedSubgraph;
import jakarta.persistence.OrderColumn;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;
import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * A row in the jobs table.
 *
 * @author amsharma
 * @author tgianos
 */
@Getter
@Setter
@ToString(
    callSuper = true,
    doNotUseGetters = true
)
@Entity
@EntityListeners(
    {
        // Can't decouple through interface or abstract class or configuration, only concrete classes work.
        JobEntityListener.class
    }
)
@Table(name = "jobs")
// Note: We're using hibernate by default it ignores fetch graphs and just loads basic fields anyway without
//       bytecode enhancement. For now just use this to load relationships and not worry too much about optimizing
//       column projection on large entity fetches
//       See:
//       https://www.baeldung.com/jpa-entity-graph#creating-entity-graph-2
//       https://stackoverflow.com/questions/37054082/hibernate-ignores-fetchgraph
@NamedEntityGraphs(
    {
        // Intended to be used as a LOAD graph
        @NamedEntityGraph(
            name = JobEntity.V3_JOB_DTO_ENTITY_GRAPH,
            attributeNodes = {
                @NamedAttributeNode("metadata"),
                @NamedAttributeNode("commandArgs"),
                @NamedAttributeNode("tags"),
                @NamedAttributeNode("imagesUsed"),
                // In actually looking at code this isn't used
                // @NamedAttributeNode("applications")
            }
        ),
        // Intended to be used as a LOAD graph
        @NamedEntityGraph(
            name = JobEntity.V4_JOB_REQUEST_DTO_ENTITY_GRAPH,
            attributeNodes = {
                @NamedAttributeNode("metadata"),
                @NamedAttributeNode("commandArgs"),
                @NamedAttributeNode("tags"),
                @NamedAttributeNode("requestedEnvironmentVariables"),
                @NamedAttributeNode("requestedAgentEnvironmentExt"),
                @NamedAttributeNode("requestedAgentConfigExt"),
                @NamedAttributeNode("setupFile"),
                @NamedAttributeNode(
                    value = "clusterCriteria",
                    subgraph = "criterion-sub-graph"
                ),
                @NamedAttributeNode(
                    value = "commandCriterion",
                    subgraph = "criterion-sub-graph"
                ),
                @NamedAttributeNode("dependencies"),
                @NamedAttributeNode("configs"),
                @NamedAttributeNode("requestedApplications"),
                @NamedAttributeNode("requestedLauncherExt"),
                @NamedAttributeNode("requestedImages"),
            },
            subgraphs = {
                @NamedSubgraph(
                    name = "criterion-sub-graph",
                    attributeNodes = {
                        @NamedAttributeNode("tags")
                    }
                ),
            }
        ),
        // Intended to be used as a LOAD graph
        @NamedEntityGraph(
            name = JobEntity.V4_JOB_SPECIFICATION_DTO_ENTITY_GRAPH,
            attributeNodes = {
                @NamedAttributeNode("configs"),
                @NamedAttributeNode("setupFile"),
                @NamedAttributeNode("commandArgs"),
                @NamedAttributeNode(
                    value = "cluster",
                    subgraph = "resource-sub-graph"
                ),
                @NamedAttributeNode(
                    value = "command",
                    subgraph = "command-sub-graph"
                ),
                @NamedAttributeNode(
                    value = "applications",
                    subgraph = "resource-sub-graph"
                ),
            },
            subgraphs = {
                @NamedSubgraph(
                    name = "resource-sub-graph",
                    attributeNodes = {
                        @NamedAttributeNode("setupFile"),
                        @NamedAttributeNode("configs"),
                        @NamedAttributeNode("dependencies")
                    }
                ),
                @NamedSubgraph(
                    name = "command-sub-graph",
                    attributeNodes = {
                        @NamedAttributeNode("executable"),
                        @NamedAttributeNode("setupFile"),
                        @NamedAttributeNode("configs"),
                        @NamedAttributeNode("dependencies")
                    }
                ),
            }
        ),
        // Intended to be used as a LOAD graph
        @NamedEntityGraph(
            name = JobEntity.JOB_APPLICATIONS_DTO_ENTITY_GRAPH,
            attributeNodes = {
                @NamedAttributeNode(
                    value = "applications",
                    subgraph = "application-sub-graph"
                )
            },
            subgraphs = {
                @NamedSubgraph(
                    name = "application-sub-graph",
                    attributeNodes = {
                        @NamedAttributeNode("setupFile"),
                        @NamedAttributeNode("configs"),
                        @NamedAttributeNode("dependencies"),
                        @NamedAttributeNode("tags")
                    }
                )
            }
        ),
        // Intended to be used as a LOAD graph
        @NamedEntityGraph(
            name = JobEntity.JOB_CLUSTER_DTO_ENTITY_GRAPH,
            attributeNodes = {
                @NamedAttributeNode(
                    value = "cluster",
                    subgraph = "cluster-sub-graph"
                ),
            },
            subgraphs = {
                @NamedSubgraph(
                    name = "cluster-sub-graph",
                    attributeNodes = {
                        @NamedAttributeNode("setupFile"),
                        @NamedAttributeNode("configs"),
                        @NamedAttributeNode("dependencies"),
                        @NamedAttributeNode("tags"),
                    }
                ),
            }
        ),
        // Intended to be used as a LOAD graph
        @NamedEntityGraph(
            name = JobEntity.JOB_COMMAND_DTO_ENTITY_GRAPH,
            attributeNodes = {
                @NamedAttributeNode(
                    value = "command",
                    subgraph = "command-sub-graph"
                )
            },
            subgraphs = {
                @NamedSubgraph(
                    name = "command-sub-graph",
                    attributeNodes = {
                        @NamedAttributeNode("executable"),
                        @NamedAttributeNode("setupFile"),
                        @NamedAttributeNode("configs"),
                        @NamedAttributeNode("dependencies"),
                        @NamedAttributeNode("tags"),
                        @NamedAttributeNode(
                            value = "clusterCriteria",
                            subgraph = "criteria-sub-graph"
                        )
                    }
                ),
                @NamedSubgraph(
                    name = "criteria-sub-graph",
                    attributeNodes = {
                        @NamedAttributeNode("tags")
                    }
                )
            }
        )
    }
)
public class JobEntity extends BaseEntity implements
    FinishedJobProjection,
    JobProjection,
    JobMetadataProjection,
    JobExecutionProjection,
    JobApplicationsProjection,
    JobClusterProjection,
    JobCommandProjection,
    JobSearchProjection,
    JobRequestProjection,
    JobSpecificationProjection,
    JobArchiveLocationProjection,
    JobApiProjection,
    StatusProjection {

    /**
     * The name of the {@link jakarta.persistence.EntityGraph} which will load all the data needed
     * for a V3 Job DTO.
     */
    public static final String V3_JOB_DTO_ENTITY_GRAPH = "Job.v3.dto.job";

    /**
     * The name of the {@link jakarta.persistence.EntityGraph} which will load all the data needed
     * for a V4 Job Request DTO.
     */
    public static final String V4_JOB_REQUEST_DTO_ENTITY_GRAPH = "Job.v4.dto.request";

    /**
     * The name of the {@link jakarta.persistence.EntityGraph} which will load all the data needed
     * for a V4 Job Specification DTO.
     */
    public static final String V4_JOB_SPECIFICATION_DTO_ENTITY_GRAPH = "Job.v4.dto.specification";

    /**
     * The name of the {@link jakarta.persistence.EntityGraph} which will load all the data needed to get the
     * applications for a job.
     */
    public static final String JOB_APPLICATIONS_DTO_ENTITY_GRAPH = "Job.applications";

    /**
     * The name of the {@link jakarta.persistence.EntityGraph} which will load all the data needed to get the
     * cluster for a job.
     */
    public static final String JOB_CLUSTER_DTO_ENTITY_GRAPH = "Job.cluster";

    /**
     * The name of the {@link jakarta.persistence.EntityGraph} which will load all the data needed to get the
     * command for a job.
     */
    public static final String JOB_COMMAND_DTO_ENTITY_GRAPH = "Job.command";

    private static final long serialVersionUID = 2849367731657512224L;

    // TODO: Drop this column once search implemented via better mechanism
    @Basic
    @Column(name = "tags", length = 1024, updatable = false)
    @Size(max = 1024, message = "Max length in database is 1024 characters")
    @Getter(AccessLevel.PACKAGE)
    @Setter(AccessLevel.NONE)
    private String tagSearchString;

    @Basic
    @Column(name = "genie_user_group", updatable = false)
    @Size(max = 255, message = "Max length in database is 255 characters")
    private String genieUserGroup;

    @Basic(optional = false)
    @Column(name = "archiving_disabled", nullable = false, updatable = false)
    private boolean archivingDisabled;

    @Basic
    @Column(name = "email", updatable = false)
    @Email
    @Size(max = 255, message = "Max length in database is 255 characters")
    private String email;

    @Basic
    @Column(name = "requested_cpu", updatable = false)
    @Min(value = 1, message = "Can't have less than 1 CPU")
    private Integer requestedCpu;

    @Basic
    @Column(name = "cpu_used")
    @Min(value = 1, message = "Can't have less than 1 CPU")
    private Integer cpuUsed;

    @Basic
    @Column(name = "requested_gpu", updatable = false)
    @Min(value = 0, message = "Can't have less than 0 GPU")
    private Integer requestedGpu;

    @Basic
    @Column(name = "gpu_used")
    @Min(value = 0, message = "Can't have less than 0 GPU")
    private Integer gpuUsed;

    @Column(name = "requested_memory", updatable = false)
    // Memory that stored in DB has data type of Integer and unit type of MB. If the requestedMemory retrieved from db
    // is 2048, it means 2048 MB, NOT 2048 Bytes.
    @Convert(converter = IntegerToLongConverter.class)
    @Min(value = 1, message = "Can't have less than 1 MB of memory allocated")
    private Long requestedMemory;

    @Column(name = "memory_used")
    // Memory that stored in DB has data type of Integer and unit type of MB. If the memoryUsed retrieved from db
    // is 2048, it means 2048 MB, NOT 2048 Bytes.
    @Convert(converter = IntegerToLongConverter.class)
    @Min(value = 1, message = "Can't have less than 1 MB of memory allocated")
    private Long memoryUsed;

    @Basic
    @Column(name = "requested_disk_mb", updatable = false)
    @Min(value = 1, message = "Can't have less than 1 MB of disk space")
    private Long requestedDiskMb;

    @Basic
    @Column(name = "disk_mb_used")
    @Min(value = 1, message = "Can't have less than 1 MB of disk space")
    private Long diskMbUsed;

    @Basic
    @Column(name = "requested_network_mbps", updatable = false)
    @Min(value = 1, message = "Can't have less than 1 MBPS of network")
    private Long requestedNetworkMbps;

    @Basic
    @Column(name = "network_mbps_used")
    @Min(value = 1, message = "Can't have less than 1 MBPS of network")
    private Long networkMbpsUsed;

    @Basic
    @Column(name = "requested_timeout", updatable = false)
    @Min(value = 1)
    private Integer requestedTimeout;

    @Basic
    @Column(name = "timeout_used")
    @Min(value = 1)
    private Integer timeoutUsed;

    @Basic
    @Column(name = "grouping", updatable = false)
    @Size(max = 255, message = "Max length in database is 255 characters")
    private String grouping;

    @Basic
    @Column(name = "grouping_instance", updatable = false)
    @Size(max = 255, message = "Max length in database is 255 characters")
    private String groupingInstance;

    @Basic
    @Column(name = "request_api_client_hostname", updatable = false)
    @Size(max = 255, message = "Max length in database is 255 characters")
    private String requestApiClientHostname;

    @Basic
    @Column(name = "request_api_client_user_agent", length = 1024, updatable = false)
    @Size(max = 1024, message = "Max length in database is 1024 characters")
    private String requestApiClientUserAgent;

    @Basic
    @Column(name = "request_agent_client_hostname", updatable = false)
    @Size(max = 255, message = "Max length in database is 255 characters")
    private String requestAgentClientHostname;

    @Basic
    @Column(name = "request_agent_client_version", updatable = false)
    @Size(max = 255, message = "Max length in database is 255 characters")
    private String requestAgentClientVersion;

    @Basic
    @Column(name = "request_agent_client_pid", updatable = false)
    @Min(value = 0, message = "Agent Client Pid can't be less than zero")
    private Integer requestAgentClientPid;

    @Basic
    @Column(name = "num_attachments", updatable = false)
    @Min(value = 0, message = "Can't have less than zero attachments")
    private Integer numAttachments;

    @Basic
    @Column(name = "total_size_of_attachments", updatable = false)
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

    @Basic
    @Column(name = "cluster_name")
    @Size(max = 255, message = "Max length in database is 255 characters")
    private String clusterName;

    @Basic
    @Column(name = "command_name")
    @Size(max = 255, message = "Max length in database is 255 characters")
    private String commandName;

    @Basic
    @Column(name = "status_msg")
    @Size(max = 255, message = "Max length in database is 255 characters")
    private String statusMsg;

    @Basic
    @Column(name = "started")
    private Instant started;

    @Basic
    @Column(name = "finished")
    private Instant finished;

    @Basic
    @Column(name = "agent_hostname")
    @Size(max = 255, message = "An agent hostname can be no longer than 255 characters")
    private String agentHostname;

    @Basic
    @Column(name = "agent_version")
    @Size(max = 255, message = "An agent version can be no longer than 255 characters")
    private String agentVersion;

    @Basic
    @Column(name = "agent_pid")
    @Min(0)
    private Integer agentPid;

    @Basic
    @Column(name = "process_id")
    private Integer processId;

    @Basic
    @Column(name = "exit_code")
    private Integer exitCode;

    @Basic
    @Column(name = "archive_location", length = 1024)
    @Size(max = 1024, message = "Max length in database is 1024 characters")
    private String archiveLocation;

    @Basic(optional = false)
    @Column(name = "interactive", nullable = false, updatable = false)
    private boolean interactive;

    @Basic(optional = false)
    @Column(name = "resolved", nullable = false)
    private boolean resolved;

    @Basic(optional = false)
    @Column(name = "claimed", nullable = false)
    private boolean claimed;

    @Basic
    @Column(name = "requested_job_directory_location", length = 1024, updatable = false)
    private String requestedJobDirectoryLocation;

    @Basic
    @Column(name = "job_directory_location", length = 1024)
    private String jobDirectoryLocation;

    @Lob
    @Basic(fetch = FetchType.LAZY)
    @Column(name = "requested_agent_config_ext", updatable = false, columnDefinition = "TEXT DEFAULT NULL")
    @Convert(converter = JsonAttributeConverter.class)
    @ToString.Exclude
    private JsonNode requestedAgentConfigExt;

    @Lob
    @Basic(fetch = FetchType.LAZY)
    @Column(name = "requested_agent_environment_ext", updatable = false, columnDefinition = "TEXT DEFAULT NULL")
    @Convert(converter = JsonAttributeConverter.class)
    @ToString.Exclude
    private JsonNode requestedAgentEnvironmentExt;

    @Basic(optional = false)
    @Column(name = "api", nullable = false)
    private boolean api = true;

    @Basic
    @Column(name = "archive_status", length = 20)
    private String archiveStatus;

    @Lob
    @Basic(fetch = FetchType.LAZY)
    @Column(name = "requested_images", columnDefinition = "TEXT DEFAULT NULL")
    @Convert(converter = JsonAttributeConverter.class)
    @ToString.Exclude
    private JsonNode requestedImages;

    @Lob
    @Basic(fetch = FetchType.LAZY)
    @Column(name = "images_used", columnDefinition = "TEXT DEFAULT NULL")
    @Convert(converter = JsonAttributeConverter.class)
    @ToString.Exclude
    private JsonNode imagesUsed;

    @Lob
    @Basic(fetch = FetchType.LAZY)
    @Column(name = "requested_launcher_ext", columnDefinition = "TEXT DEFAULT NULL")
    @Convert(converter = JsonAttributeConverter.class)
    @ToString.Exclude
    private JsonNode requestedLauncherExt;

    @Lob
    @Basic(fetch = FetchType.LAZY)
    @Column(name = "launcher_ext", columnDefinition = "TEXT DEFAULT NULL")
    @Convert(converter = JsonAttributeConverter.class)
    @ToString.Exclude
    private JsonNode launcherExt;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "cluster_id")
    @ToString.Exclude
    private ClusterEntity cluster;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "command_id")
    @ToString.Exclude
    private CommandEntity command;

    @ElementCollection
    @CollectionTable(
        name = "job_command_arguments",
        joinColumns = {
            @JoinColumn(name = "job_id", nullable = false)
        }
    )
    @Column(name = "argument", length = 10_000, nullable = false)
    @OrderColumn(name = "argument_order", nullable = false, updatable = false)
    @ToString.Exclude
    private List<@NotBlank @Size(max = 10_000) String> commandArgs = new ArrayList<>();

    @ElementCollection(fetch = FetchType.LAZY)
    @CollectionTable(
        name = "job_requested_environment_variables",
        joinColumns = {
            @JoinColumn(name = "job_id", nullable = false)
        }
    )
    @MapKeyColumn(name = "name", updatable = false)
    @Column(name = "value", length = 1024, nullable = false)
    @ToString.Exclude
    private Map<@NotBlank @Size(max = 255) String, @NotNull @Size(max = 1024) String>
        requestedEnvironmentVariables = new HashMap<>();

    @ElementCollection(fetch = FetchType.LAZY)
    @CollectionTable(
        name = "job_environment_variables",
        joinColumns = {
            @JoinColumn(name = "job_id", nullable = false)
        }
    )
    @MapKeyColumn(name = "name")
    @Column(name = "value", length = 1024, nullable = false)
    @ToString.Exclude
    private Map<@NotBlank @Size(max = 255) String, @NotNull @Size(max = 1024) String>
        environmentVariables = new HashMap<>();

    @ManyToMany(fetch = FetchType.LAZY)
    @JoinTable(
        name = "jobs_applications",
        joinColumns = {
            @JoinColumn(name = "job_id", referencedColumnName = "id", nullable = false)
        },
        inverseJoinColumns = {
            @JoinColumn(name = "application_id", referencedColumnName = "id", nullable = false)
        }
    )
    @OrderColumn(name = "application_order", nullable = false, updatable = false)
    @ToString.Exclude
    private List<ApplicationEntity> applications = new ArrayList<>();

    @ManyToMany(fetch = FetchType.LAZY, cascade = CascadeType.ALL)
    @JoinTable(
        name = "jobs_cluster_criteria",
        joinColumns = {
            @JoinColumn(name = "job_id", referencedColumnName = "id", nullable = false)
        },
        inverseJoinColumns = {
            @JoinColumn(name = "criterion_id", referencedColumnName = "id", nullable = false)
        }
    )
    @OrderColumn(name = "priority_order", nullable = false, updatable = false)
    @ToString.Exclude
    private List<CriterionEntity> clusterCriteria = new ArrayList<>();

    @ManyToOne(fetch = FetchType.LAZY, cascade = CascadeType.ALL)
    @JoinColumn(name = "command_criterion", nullable = false)
    @ToString.Exclude
    private CriterionEntity commandCriterion;

    @ElementCollection
    @CollectionTable(
        name = "job_requested_applications",
        joinColumns = {
            @JoinColumn(name = "job_id", nullable = false)
        }
    )
    @Column(name = "application_id", nullable = false)
    @OrderColumn(name = "application_order", nullable = false)
    @ToString.Exclude
    private List<String> requestedApplications = new ArrayList<>();

    @ManyToMany(fetch = FetchType.LAZY)
    @JoinTable(
        name = "jobs_configs",
        joinColumns = {
            @JoinColumn(name = "job_id", referencedColumnName = "id", nullable = false)
        },
        inverseJoinColumns = {
            @JoinColumn(name = "file_id", referencedColumnName = "id", nullable = false)
        }
    )
    @ToString.Exclude
    private Set<FileEntity> configs = new HashSet<>();

    @ManyToMany(fetch = FetchType.LAZY)
    @JoinTable(
        name = "jobs_dependencies",
        joinColumns = {
            @JoinColumn(name = "job_id", referencedColumnName = "id", nullable = false)
        },
        inverseJoinColumns = {
            @JoinColumn(name = "file_id", referencedColumnName = "id", nullable = false)
        }
    )
    @ToString.Exclude
    private Set<FileEntity> dependencies = new HashSet<>();

    @ManyToMany(fetch = FetchType.LAZY)
    @JoinTable(
        name = "jobs_tags",
        joinColumns = {
            @JoinColumn(name = "job_id", referencedColumnName = "id", nullable = false)
        },
        inverseJoinColumns = {
            @JoinColumn(name = "tag_id", referencedColumnName = "id", nullable = false)
        }
    )
    @ToString.Exclude
    private Set<TagEntity> tags = new HashSet<>();

    @Transient
    @ToString.Exclude
    private String notifiedJobStatus;

    /**
     * Default Constructor.
     */
    public JobEntity() {
        super();
    }

    /**
     * Before a job is created, create the job search string.
     */
    void onCreateJob() {
        if (!this.tags.isEmpty()) {
            // Tag search string length max is currently 1024 which will be caught by hibernate validator if this
            // exceeds that length
            this.tagSearchString = PredicateUtils.createTagSearchString(this.tags);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<String> getGenieUserGroup() {
        return Optional.ofNullable(this.genieUserGroup);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<String> getEmail() {
        return Optional.ofNullable(this.email);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<Integer> getRequestedCpu() {
        return Optional.ofNullable(this.requestedCpu);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<Integer> getRequestedTimeout() {
        return Optional.ofNullable(this.requestedTimeout);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<Integer> getCpuUsed() {
        return Optional.ofNullable(this.cpuUsed);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<Integer> getGpuUsed() {
        return Optional.ofNullable(this.gpuUsed);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<Long> getDiskMbUsed() {
        return Optional.ofNullable(this.diskMbUsed);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<Long> getNetworkMbpsUsed() {
        return Optional.ofNullable(this.networkMbpsUsed);
    }

    /**
     * Set the command criterion.
     *
     * @param commandCriterion The criterion. Null clears reference.
     */
    public void setCommandCriterion(@Nullable final CriterionEntity commandCriterion) {
        this.commandCriterion = commandCriterion;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<String> getGrouping() {
        return Optional.ofNullable(this.grouping);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<String> getGroupingInstance() {
        return Optional.ofNullable(this.groupingInstance);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<String> getStatusMsg() {
        return Optional.ofNullable(this.statusMsg);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<Instant> getStarted() {
        return Optional.ofNullable(this.started);
    }

    /**
     * Set the start time for the job.
     *
     * @param started The started time.
     */
    public void setStarted(@Nullable final Instant started) {
        this.started = started;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<Instant> getFinished() {
        return Optional.ofNullable(this.finished);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<Long> getRequestedMemory() {
        return Optional.ofNullable(this.requestedMemory);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<String> getRequestApiClientHostname() {
        return Optional.ofNullable(this.requestApiClientHostname);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<String> getRequestApiClientUserAgent() {
        return Optional.ofNullable(this.requestApiClientUserAgent);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<Integer> getNumAttachments() {
        return Optional.ofNullable(this.numAttachments);
    }

    /**
     * Get the hostname of the agent that requested this job be run if there was one.
     *
     * @return The hostname wrapped in an {@link Optional}
     */
    public Optional<String> getRequestAgentClientHostname() {
        return Optional.ofNullable(this.requestAgentClientHostname);
    }

    /**
     * Get the version of the agent that requested this job be run if there was one.
     *
     * @return The version wrapped in an {@link Optional}
     */
    public Optional<String> getRequestAgentClientVersion() {
        return Optional.ofNullable(this.requestAgentClientVersion);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<Integer> getExitCode() {
        return Optional.ofNullable(this.exitCode);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<String> getArchiveLocation() {
        return Optional.ofNullable(this.archiveLocation);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<Long> getMemoryUsed() {
        return Optional.ofNullable(this.memoryUsed);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<ClusterEntity> getCluster() {
        return Optional.ofNullable(this.cluster);
    }

    /**
     * Set the cluster this job ran on.
     *
     * @param cluster The cluster this job ran on
     */
    public void setCluster(@Nullable final ClusterEntity cluster) {
        if (this.cluster != null) {
            this.clusterName = null;
        }

        this.cluster = cluster;

        if (this.cluster != null) {
            this.clusterName = cluster.getName();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<CommandEntity> getCommand() {
        return Optional.ofNullable(this.command);
    }

    /**
     * Set the command used to run this job.
     *
     * @param command The command
     */
    public void setCommand(@Nullable final CommandEntity command) {
        if (this.command != null) {
            this.commandName = null;
        }

        this.command = command;

        if (this.command != null) {
            this.commandName = command.getName();
        }
    }

    /**
     * Set the finishTime for the job.
     *
     * @param finished The finished time.
     */
    public void setFinished(@Nullable final Instant finished) {
        this.finished = finished;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<Long> getTotalSizeOfAttachments() {
        return Optional.ofNullable(this.totalSizeOfAttachments);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<Long> getStdOutSize() {
        return Optional.ofNullable(this.stdOutSize);
    }

    /**
     * Set the total size in bytes of the std out file for this job.
     *
     * @param stdOutSize The size. Null empty's database field
     */
    public void setStdOutSize(@Nullable final Long stdOutSize) {
        this.stdOutSize = stdOutSize;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<Long> getStdErrSize() {
        return Optional.ofNullable(this.stdErrSize);
    }

    /**
     * Set the total size in bytes of the std err file for this job.
     *
     * @param stdErrSize The size. Null empty's database field
     */
    public void setStdErrSize(@Nullable final Long stdErrSize) {
        this.stdErrSize = stdErrSize;
    }

    /**
     * Get the PID of the agent that requested this job be run if there was one.
     *
     * @return The PID wrapped in an {@link Optional}
     */
    public Optional<Integer> getRequestAgentClientPid() {
        return Optional.ofNullable(this.requestAgentClientPid);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<String> getClusterName() {
        return Optional.ofNullable(this.clusterName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<String> getCommandName() {
        return Optional.ofNullable(this.commandName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<String> getAgentHostname() {
        return Optional.ofNullable(this.agentHostname);
    }

    /**
     * Get the version of the agent that claimed this job.
     *
     * @return The version wrapped in an {@link Optional} in case it wasn't set yet in which case it will be
     * {@link Optional#empty()}
     */
    public Optional<String> getAgentVersion() {
        return Optional.ofNullable(this.agentVersion);
    }

    /**
     * Get the pid of the agent that claimed this job.
     *
     * @return The pid wrapped in an {@link Optional} in case it wasn't set yet in which case it will be
     * {@link Optional#empty()}
     */
    public Optional<Integer> getAgentPid() {
        return Optional.ofNullable(this.agentPid);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<Integer> getProcessId() {
        return Optional.ofNullable(this.processId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<Integer> getTimeoutUsed() {
        return Optional.ofNullable(this.timeoutUsed);
    }

    /**
     * Set the final resolved timeout duration for this job.
     *
     * @param timeoutUsed The timeout value (in seconds) after which this job should be killed by the system
     */
    public void setTimeoutUsed(@Nullable final Integer timeoutUsed) {
        this.timeoutUsed = timeoutUsed;
    }

    /**
     * Get the archive status for the job if there is one.
     *
     * @return The archive status or {@link Optional#empty()} if there is none
     */
    public Optional<String> getArchiveStatus() {
        return Optional.ofNullable(this.archiveStatus);
    }

    /**
     * Set the archive status for this job.
     *
     * @param archiveStatus The new archive status
     */
    public void setArchiveStatus(@Nullable final String archiveStatus) {
        this.archiveStatus = archiveStatus;
    }

    /**
     * Get any metadata associated with this job pertaining to the launcher that launched it.
     *
     * @return The metadata or {@link Optional#empty()} if there isn't any
     */
    public Optional<JsonNode> getLauncherExt() {
        return Optional.ofNullable(this.launcherExt);
    }

    /**
     * Set any metadata pertaining to the launcher that launched this job.
     *
     * @param launcherExt The metadata
     */
    public void setLauncherExt(@Nullable final JsonNode launcherExt) {
        this.launcherExt = launcherExt;
    }

    /**
     * Set the command arguments to use with this job.
     *
     * @param commandArgs The command arguments to use
     */
    public void setCommandArgs(@Nullable final List<String> commandArgs) {
        this.commandArgs.clear();
        if (commandArgs != null) {
            this.commandArgs.addAll(commandArgs);
        }
    }

    /**
     * Set all the files associated as configuration files for this job.
     *
     * @param configs The configuration files to set
     */
    public void setConfigs(@Nullable final Set<FileEntity> configs) {
        this.configs.clear();
        if (configs != null) {
            this.configs.addAll(configs);
        }
    }

    /**
     * Set all the files associated as dependency files for this job.
     *
     * @param dependencies The dependency files to set
     */
    public void setDependencies(@Nullable final Set<FileEntity> dependencies) {
        this.dependencies.clear();
        if (dependencies != null) {
            this.dependencies.addAll(dependencies);
        }
    }

    /**
     * Set all the tags associated to this job.
     *
     * @param tags The tags to set
     */
    public void setTags(@Nullable final Set<TagEntity> tags) {
        this.tags.clear();
        if (tags != null) {
            this.tags.addAll(tags);
        }
    }

    /**
     * Set the requested environment variables.
     *
     * @param requestedEnvironmentVariables The environment variables the user requested be added to the job runtime
     */
    public void setRequestedEnvironmentVariables(@Nullable final Map<String, String> requestedEnvironmentVariables) {
        this.requestedEnvironmentVariables.clear();
        if (requestedEnvironmentVariables != null) {
            this.requestedEnvironmentVariables.putAll(requestedEnvironmentVariables);
        }
    }

    /**
     * Set the environment variables for the job.
     *
     * @param environmentVariables The final set of environment variables that were set in the job runtime
     */
    public void setEnvironmentVariables(@Nullable final Map<String, String> environmentVariables) {
        this.environmentVariables.clear();
        if (environmentVariables != null) {
            this.environmentVariables.putAll(environmentVariables);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<String> getRequestedJobDirectoryLocation() {
        return Optional.ofNullable(this.requestedJobDirectoryLocation);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<JsonNode> getRequestedAgentEnvironmentExt() {
        return Optional.ofNullable(this.requestedAgentEnvironmentExt);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<JsonNode> getRequestedAgentConfigExt() {
        return Optional.ofNullable(this.requestedAgentConfigExt);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<Integer> getRequestedGpu() {
        return Optional.ofNullable(this.requestedGpu);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<Long> getRequestedDiskMb() {
        return Optional.ofNullable(this.requestedDiskMb);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<Long> getRequestedNetworkMbps() {
        return Optional.ofNullable(this.requestedNetworkMbps);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<JsonNode> getRequestedImages() {
        return Optional.ofNullable(this.requestedImages);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<String> getJobDirectoryLocation() {
        return Optional.ofNullable(this.jobDirectoryLocation);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<JsonNode> getImagesUsed() {
        return Optional.ofNullable(this.imagesUsed);
    }

    /**
     * Set the applications used to run this job.
     *
     * @param applications The applications
     */
    public void setApplications(@Nullable final List<ApplicationEntity> applications) {
        this.applications.clear();
        if (applications != null) {
            this.applications.addAll(applications);
        }
    }

    /**
     * Set the cluster criteria set for this job.
     *
     * @param clusterCriteria The cluster criteria in priority order
     */
    public void setClusterCriteria(@Nullable final List<CriterionEntity> clusterCriteria) {
        this.clusterCriteria.clear();
        if (clusterCriteria != null) {
            this.clusterCriteria.addAll(clusterCriteria);
        }
    }

    /**
     * Get the previously notified job status if there was one.
     *
     * @return The previously notified job status wrapped in an {@link Optional} or {@link Optional#empty()}
     */
    public Optional<String> getNotifiedJobStatus() {
        return Optional.ofNullable(this.notifiedJobStatus);
    }

    /**
     * Get any metadata the user provided with respect to the launcher.
     *
     * @return The metadata or {@link Optional#empty()} if there isn't any
     */
    public Optional<JsonNode> getRequestedLauncherExt() {
        return Optional.ofNullable(this.requestedLauncherExt);
    }

    /**
     * Set any metadata pertaining to the launcher that was provided by the user.
     *
     * @param requestedLauncherExt The metadata
     */
    public void setRequestedLauncherExt(@Nullable final JsonNode requestedLauncherExt) {
        this.requestedLauncherExt = requestedLauncherExt;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(final Object o) {
        return super.equals(o);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return super.hashCode();
    }
}
