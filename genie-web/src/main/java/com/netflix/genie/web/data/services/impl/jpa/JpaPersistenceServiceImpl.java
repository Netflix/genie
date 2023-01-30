/*
 *
 *  Copyright 2020 Netflix, Inc.
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
package com.netflix.genie.web.data.services.impl.jpa;

import brave.SpanCustomizer;
import brave.Tracer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.Job;
import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.common.dto.UserResourcesSummary;
import com.netflix.genie.common.dto.search.JobSearchResult;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.common.external.util.GenieObjectMapper;
import com.netflix.genie.common.internal.dtos.AgentClientMetadata;
import com.netflix.genie.common.internal.dtos.AgentConfigRequest;
import com.netflix.genie.common.internal.dtos.Application;
import com.netflix.genie.common.internal.dtos.ApplicationMetadata;
import com.netflix.genie.common.internal.dtos.ApplicationRequest;
import com.netflix.genie.common.internal.dtos.ApplicationStatus;
import com.netflix.genie.common.internal.dtos.ArchiveStatus;
import com.netflix.genie.common.internal.dtos.Cluster;
import com.netflix.genie.common.internal.dtos.ClusterMetadata;
import com.netflix.genie.common.internal.dtos.ClusterRequest;
import com.netflix.genie.common.internal.dtos.ClusterStatus;
import com.netflix.genie.common.internal.dtos.Command;
import com.netflix.genie.common.internal.dtos.CommandMetadata;
import com.netflix.genie.common.internal.dtos.CommandRequest;
import com.netflix.genie.common.internal.dtos.CommandStatus;
import com.netflix.genie.common.internal.dtos.CommonMetadata;
import com.netflix.genie.common.internal.dtos.CommonResource;
import com.netflix.genie.common.internal.dtos.ComputeResources;
import com.netflix.genie.common.internal.dtos.Criterion;
import com.netflix.genie.common.internal.dtos.ExecutionEnvironment;
import com.netflix.genie.common.internal.dtos.ExecutionResourceCriteria;
import com.netflix.genie.common.internal.dtos.FinishedJob;
import com.netflix.genie.common.internal.dtos.Image;
import com.netflix.genie.common.internal.dtos.JobEnvironment;
import com.netflix.genie.common.internal.dtos.JobEnvironmentRequest;
import com.netflix.genie.common.internal.dtos.JobMetadata;
import com.netflix.genie.common.internal.dtos.JobRequest;
import com.netflix.genie.common.internal.dtos.JobRequestMetadata;
import com.netflix.genie.common.internal.dtos.JobSpecification;
import com.netflix.genie.common.internal.dtos.JobStatus;
import com.netflix.genie.common.internal.dtos.converters.DtoConverters;
import com.netflix.genie.common.internal.exceptions.checked.GenieCheckedException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieInvalidStatusException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieJobAlreadyClaimedException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieRuntimeException;
import com.netflix.genie.common.internal.tracing.TracingConstants;
import com.netflix.genie.common.internal.tracing.brave.BraveTagAdapter;
import com.netflix.genie.common.internal.tracing.brave.BraveTracingComponents;
import com.netflix.genie.web.data.services.PersistenceService;
import com.netflix.genie.web.data.services.impl.jpa.converters.EntityV3DtoConverters;
import com.netflix.genie.web.data.services.impl.jpa.converters.EntityV4DtoConverters;
import com.netflix.genie.web.data.services.impl.jpa.entities.ApplicationEntity;
import com.netflix.genie.web.data.services.impl.jpa.entities.ApplicationEntity_;
import com.netflix.genie.web.data.services.impl.jpa.entities.BaseEntity;
import com.netflix.genie.web.data.services.impl.jpa.entities.ClusterEntity;
import com.netflix.genie.web.data.services.impl.jpa.entities.ClusterEntity_;
import com.netflix.genie.web.data.services.impl.jpa.entities.CommandEntity;
import com.netflix.genie.web.data.services.impl.jpa.entities.CommandEntity_;
import com.netflix.genie.web.data.services.impl.jpa.entities.CriterionEntity;
import com.netflix.genie.web.data.services.impl.jpa.entities.FileEntity;
import com.netflix.genie.web.data.services.impl.jpa.entities.JobEntity;
import com.netflix.genie.web.data.services.impl.jpa.entities.JobEntity_;
import com.netflix.genie.web.data.services.impl.jpa.entities.TagEntity;
import com.netflix.genie.web.data.services.impl.jpa.entities.UniqueIdEntity;
import com.netflix.genie.web.data.services.impl.jpa.queries.aggregates.JobInfoAggregate;
import com.netflix.genie.web.data.services.impl.jpa.queries.predicates.ApplicationPredicates;
import com.netflix.genie.web.data.services.impl.jpa.queries.predicates.ClusterPredicates;
import com.netflix.genie.web.data.services.impl.jpa.queries.predicates.CommandPredicates;
import com.netflix.genie.web.data.services.impl.jpa.queries.predicates.JobPredicates;
import com.netflix.genie.web.data.services.impl.jpa.queries.projections.JobExecutionProjection;
import com.netflix.genie.web.data.services.impl.jpa.queries.projections.JobMetadataProjection;
import com.netflix.genie.web.data.services.impl.jpa.queries.projections.v4.FinishedJobProjection;
import com.netflix.genie.web.data.services.impl.jpa.queries.projections.v4.JobSpecificationProjection;
import com.netflix.genie.web.data.services.impl.jpa.repositories.JpaApplicationRepository;
import com.netflix.genie.web.data.services.impl.jpa.repositories.JpaBaseRepository;
import com.netflix.genie.web.data.services.impl.jpa.repositories.JpaClusterRepository;
import com.netflix.genie.web.data.services.impl.jpa.repositories.JpaCommandRepository;
import com.netflix.genie.web.data.services.impl.jpa.repositories.JpaCriterionRepository;
import com.netflix.genie.web.data.services.impl.jpa.repositories.JpaFileRepository;
import com.netflix.genie.web.data.services.impl.jpa.repositories.JpaJobRepository;
import com.netflix.genie.web.data.services.impl.jpa.repositories.JpaRepositories;
import com.netflix.genie.web.data.services.impl.jpa.repositories.JpaTagRepository;
import com.netflix.genie.web.dtos.JobSubmission;
import com.netflix.genie.web.dtos.ResolvedJob;
import com.netflix.genie.web.exceptions.checked.IdAlreadyExistsException;
import com.netflix.genie.web.exceptions.checked.NotFoundException;
import com.netflix.genie.web.exceptions.checked.PreconditionFailedException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.persistence.EntityManager;
import javax.persistence.NoResultException;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Order;
import javax.persistence.criteria.Root;
import javax.persistence.criteria.Subquery;
import javax.validation.ConstraintViolationException;
import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Implementation of {@link PersistenceService} using JPA.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Transactional(
    // TODO: Double check the docs on this as default is runtime exception and error... may want to incorporate them
    rollbackFor = {
        GenieException.class,
        GenieCheckedException.class,
        GenieRuntimeException.class,
        ConstraintViolationException.class
    }
)
@Slf4j
public class JpaPersistenceServiceImpl implements PersistenceService {

    /**
     * The set of active statuses as their names.
     */
    @VisibleForTesting
    static final Set<String> ACTIVE_STATUS_SET = JobStatus
        .getActiveStatuses()
        .stream()
        .map(Enum::name)
        .collect(Collectors.toSet());

    /**
     * The set containing statuses that come before CLAIMED.
     */
    @VisibleForTesting
    static final Set<String> UNCLAIMED_STATUS_SET = JobStatus
        .getStatusesBeforeClaimed()
        .stream()
        .map(Enum::name)
        .collect(Collectors.toSet());

    /**
     * The set of job statuses which are considered to be using memory on a Genie node.
     */
    @VisibleForTesting
    static final Set<String> USING_MEMORY_JOB_SET = Stream
        .of(JobStatus.CLAIMED, JobStatus.INIT, JobStatus.RUNNING)
        .map(Enum::name)
        .collect(Collectors.toSet());

    private static final String LOAD_GRAPH_HINT = "javax.persistence.loadgraph";
    private static final int MAX_STATUS_MESSAGE_LENGTH = 255;

    private final EntityManager entityManager;

    private final JpaApplicationRepository applicationRepository;
    private final JpaClusterRepository clusterRepository;
    private final JpaCommandRepository commandRepository;
    private final JpaCriterionRepository criterionRepository;
    private final JpaFileRepository fileRepository;
    private final JpaJobRepository jobRepository;
    private final JpaTagRepository tagRepository;

    private final Tracer tracer;
    private final BraveTagAdapter tagAdapter;

    /**
     * Constructor.
     *
     * @param entityManager     The {@link EntityManager} to use
     * @param jpaRepositories   All the repositories in the Genie application
     * @param tracingComponents All the Brave related tracing components needed to add metadata to Spans
     */
    public JpaPersistenceServiceImpl(
        final EntityManager entityManager,
        final JpaRepositories jpaRepositories,
        final BraveTracingComponents tracingComponents
    ) {
        this.entityManager = entityManager;
        this.applicationRepository = jpaRepositories.getApplicationRepository();
        this.clusterRepository = jpaRepositories.getClusterRepository();
        this.commandRepository = jpaRepositories.getCommandRepository();
        this.criterionRepository = jpaRepositories.getCriterionRepository();
        this.fileRepository = jpaRepositories.getFileRepository();
        this.jobRepository = jpaRepositories.getJobRepository();
        this.tagRepository = jpaRepositories.getTagRepository();

        this.tracer = tracingComponents.getTracer();
        this.tagAdapter = tracingComponents.getTagAdapter();
    }

    //region Application APIs

    /**
     * {@inheritDoc}
     */
    @Override
    public String saveApplication(@Valid final ApplicationRequest applicationRequest) throws IdAlreadyExistsException {
        log.debug("[saveApplication] Called to save {}", applicationRequest);
        final ApplicationEntity entity = new ApplicationEntity();
        this.setUniqueId(entity, applicationRequest.getRequestedId().orElse(null));
        this.updateApplicationEntity(entity, applicationRequest.getResources(), applicationRequest.getMetadata());

        try {
            return this.applicationRepository.save(entity).getUniqueId();
        } catch (final DataIntegrityViolationException e) {
            throw new IdAlreadyExistsException(
                "An application with id " + entity.getUniqueId() + " already exists",
                e
            );
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Application getApplication(@NotBlank final String id) throws NotFoundException {
        log.debug("[getApplication] Called for {}", id);
        return EntityV4DtoConverters.toV4ApplicationDto(
            this.applicationRepository
                .getApplicationDto(id)
                .orElseThrow(() -> new NotFoundException("No application with id " + id + " exists"))
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public Page<Application> findApplications(
        @Nullable final String name,
        @Nullable final String user,
        @Nullable final Set<ApplicationStatus> statuses,
        @Nullable final Set<String> tags,
        @Nullable final String type,
        final Pageable page
    ) {
        /*
         * NOTE: This is implemented this way for a reason:
         * 1. To solve the JPA N+1 problem: https://vladmihalcea.com/n-plus-1-query-problem/
         * 2. To address this: https://vladmihalcea.com/fix-hibernate-hhh000104-entity-fetch-pagination-warning-message/
         * This reduces the number of queries from potentially 100's to 3
         */
        log.debug(
            "[findApplications] Called with name = {}, user = {}, statuses = {}, tags = {}, type = {}",
            name,
            user,
            statuses,
            tags,
            type
        );

        final Set<String> statusStrings = statuses != null
            ? statuses.stream().map(Enum::name).collect(Collectors.toSet())
            : null;

        // TODO: Still more optimization that can be done here to not load these entities
        //       Figure out how to use just strings in the predicate
        final Set<TagEntity> tagEntities = tags == null
            ? null
            : this.tagRepository.findByTagIn(tags);
        if (tagEntities != null && tagEntities.size() != tags.size()) {
            // short circuit for no results as at least one of the expected tags doesn't exist
            return new PageImpl<>(new ArrayList<>(0));
        }

        final CriteriaBuilder criteriaBuilder = this.entityManager.getCriteriaBuilder();
        final CriteriaQuery<Long> countQuery = criteriaBuilder.createQuery(Long.class);
        final Root<ApplicationEntity> countQueryRoot = countQuery.from(ApplicationEntity.class);
        final Subquery<Long> countIdSubQuery = countQuery.subquery(Long.class);
        final Root<ApplicationEntity> countIdSubQueryRoot = countIdSubQuery.from(ApplicationEntity.class);
        countIdSubQuery.select(countIdSubQueryRoot.get(ApplicationEntity_.id));
        countIdSubQuery.where(
            ApplicationPredicates.find(
                countIdSubQueryRoot,
                countIdSubQuery,
                criteriaBuilder,
                name,
                user,
                statusStrings,
                tagEntities,
                type
            )
        );
        countQuery.select(criteriaBuilder.count(countQueryRoot));
        countQuery.where(countQueryRoot.get(ApplicationEntity_.id).in(countIdSubQuery));

        final Long totalCount = this.entityManager.createQuery(countQuery).getSingleResult();
        if (totalCount == null || totalCount == 0) {
            // short circuit for no results
            return new PageImpl<>(new ArrayList<>(0));
        }

        final CriteriaQuery<Long> idQuery = criteriaBuilder.createQuery(Long.class);
        final Root<ApplicationEntity> idQueryRoot = idQuery.from(ApplicationEntity.class);
        idQuery.select(idQueryRoot.get(ApplicationEntity_.id));
        idQuery.where(
            // NOTE: The problem with trying to reuse the predicate above even though they seem the same is they have
            //       different query objects. If there is a join added by the predicate function it won't be on the
            //       right object as these criteria queries are basically builders
            ApplicationPredicates.find(
                idQueryRoot,
                idQuery,
                criteriaBuilder,
                name,
                user,
                statusStrings,
                tagEntities,
                type
            )
        );

        final Sort sort = page.getSort();
        final List<Order> orders = new ArrayList<>();
        sort.iterator().forEachRemaining(
            order -> {
                if (order.isAscending()) {
                    orders.add(criteriaBuilder.asc(idQueryRoot.get(order.getProperty())));
                } else {
                    orders.add(criteriaBuilder.desc(idQueryRoot.get(order.getProperty())));
                }
            }
        );
        idQuery.orderBy(orders);

        final List<Long> applicationIds = this.entityManager
            .createQuery(idQuery)
            .setFirstResult(((Long) page.getOffset()).intValue())
            .setMaxResults(page.getPageSize())
            .getResultList();

        final CriteriaQuery<ApplicationEntity> contentQuery = criteriaBuilder.createQuery(ApplicationEntity.class);
        final Root<ApplicationEntity> contentQueryRoot = contentQuery.from(ApplicationEntity.class);
        contentQuery.select(contentQueryRoot);
        contentQuery.where(contentQueryRoot.get(ApplicationEntity_.id).in(applicationIds));
        // Need to make the same order by or results won't be accurate
        contentQuery.orderBy(orders);

        final List<Application> applications = this.entityManager
            .createQuery(contentQuery)
            .setHint(LOAD_GRAPH_HINT, this.entityManager.getEntityGraph(ApplicationEntity.DTO_ENTITY_GRAPH))
            .getResultStream()
            .map(EntityV4DtoConverters::toV4ApplicationDto)
            .collect(Collectors.toList());

        return new PageImpl<>(applications, page, totalCount);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateApplication(
        @NotBlank final String id,
        @Valid final Application updateApp
    ) throws NotFoundException, PreconditionFailedException {
        log.debug("[updateApplication] Called to update application {} with {}", id, updateApp);
        if (!updateApp.getId().equals(id)) {
            throw new PreconditionFailedException("Application id " + id + " inconsistent with id passed in.");
        }
        this.updateApplicationEntity(
            this.applicationRepository
                .getApplicationDto(id)
                .orElseThrow(() -> new NotFoundException("No application with id " + id + " exists")),
            updateApp.getResources(),
            updateApp.getMetadata()
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteAllApplications() throws PreconditionFailedException {
        log.debug("[deleteAllApplications] Called");
        for (final ApplicationEntity entity : this.applicationRepository.findAll()) {
            this.deleteApplicationEntity(entity);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteApplication(@NotBlank final String id) throws PreconditionFailedException {
        log.debug("[deleteApplication] Called for {}", id);
        final Optional<ApplicationEntity> entity = this.applicationRepository.getApplicationAndCommands(id);
        if (entity.isEmpty()) {
            // There's nothing to do as the caller wants to delete something that doesn't exist.
            return;
        }

        this.deleteApplicationEntity(entity.get());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<Command> getCommandsForApplication(
        @NotBlank final String id,
        @Nullable final Set<CommandStatus> statuses
    ) throws NotFoundException {
        log.debug("[getCommandsForApplication] Called for application {} filtered by statuses {}", id, statuses);
        return this.applicationRepository
            .getApplicationAndCommandsDto(id)
            .orElseThrow(() -> new NotFoundException("No application with id " + id + " exists"))
            .getCommands()
            .stream()
            .filter(
                commandEntity -> statuses == null
                    || statuses.contains(DtoConverters.toV4CommandStatus(commandEntity.getStatus()))
            )
            .map(EntityV4DtoConverters::toV4CommandDto)
            .collect(Collectors.toSet());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(isolation = Isolation.READ_COMMITTED)
    public long deleteUnusedApplications(final Instant createdThreshold, final int batchSize) {
        log.info("Attempting to delete unused applications created before {}", createdThreshold);
        return this.applicationRepository.deleteByIdIn(
            this.applicationRepository.findUnusedApplications(createdThreshold, batchSize)
        );
    }
    //endregion

    //region Cluster APIs

    /**
     * {@inheritDoc}
     */
    @Override
    public String saveCluster(@Valid final ClusterRequest clusterRequest) throws IdAlreadyExistsException {
        log.debug("[saveCluster] Called to save {}", clusterRequest);
        final ClusterEntity entity = new ClusterEntity();
        this.setUniqueId(entity, clusterRequest.getRequestedId().orElse(null));
        this.updateClusterEntity(entity, clusterRequest.getResources(), clusterRequest.getMetadata());

        try {
            return this.clusterRepository.save(entity).getUniqueId();
        } catch (final DataIntegrityViolationException e) {
            throw new IdAlreadyExistsException("A cluster with id " + entity.getUniqueId() + " already exists", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Cluster getCluster(@NotBlank final String id) throws NotFoundException {
        log.debug("[getCluster] Called for {}", id);
        return EntityV4DtoConverters.toV4ClusterDto(
            this.clusterRepository
                .getClusterDto(id)
                .orElseThrow(() -> new NotFoundException("No cluster with id " + id + " exists"))
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public Page<Cluster> findClusters(
        @Nullable final String name,
        @Nullable final Set<ClusterStatus> statuses,
        @Nullable final Set<String> tags,
        @Nullable final Instant minUpdateTime,
        @Nullable final Instant maxUpdateTime,
        final Pageable page
    ) {
        /*
         * NOTE: This is implemented this way for a reason:
         * 1. To solve the JPA N+1 problem: https://vladmihalcea.com/n-plus-1-query-problem/
         * 2. To address this: https://vladmihalcea.com/fix-hibernate-hhh000104-entity-fetch-pagination-warning-message/
         * This reduces the number of queries from potentially 100's to 3
         */
        log.debug(
            "[findClusters] Called with name = {}, statuses = {}, tags = {}, minUpdateTime = {}, maxUpdateTime = {}",
            name,
            statuses,
            tags,
            minUpdateTime,
            maxUpdateTime
        );
        final Set<String> statusStrings = statuses != null
            ? statuses.stream().map(Enum::name).collect(Collectors.toSet())
            : null;

        // TODO: Still more optimization that can be done here to not load these entities
        //       Figure out how to use just strings in the predicate
        final Set<TagEntity> tagEntities = tags == null
            ? null
            : this.tagRepository.findByTagIn(tags);
        if (tagEntities != null && tagEntities.size() != tags.size()) {
            // short circuit for no results as at least one of the expected tags doesn't exist
            return new PageImpl<>(new ArrayList<>(0));
        }

        final CriteriaBuilder criteriaBuilder = this.entityManager.getCriteriaBuilder();
        final CriteriaQuery<Long> countQuery = criteriaBuilder.createQuery(Long.class);
        final Root<ClusterEntity> countQueryRoot = countQuery.from(ClusterEntity.class);
        final Subquery<Long> countIdSubQuery = countQuery.subquery(Long.class);
        final Root<ClusterEntity> countIdSubQueryRoot = countIdSubQuery.from(ClusterEntity.class);
        countIdSubQuery.select(countIdSubQueryRoot.get(ClusterEntity_.id));
        countIdSubQuery.where(
            ClusterPredicates.find(
                countIdSubQueryRoot,
                countIdSubQuery,
                criteriaBuilder,
                name,
                statusStrings,
                tagEntities,
                minUpdateTime,
                maxUpdateTime
            )
        );
        countQuery.select(criteriaBuilder.count(countQueryRoot));
        countQuery.where(countQueryRoot.get(ClusterEntity_.id).in(countIdSubQuery));

        final Long totalCount = this.entityManager.createQuery(countQuery).getSingleResult();
        if (totalCount == null || totalCount == 0) {
            // short circuit for no results
            return new PageImpl<>(new ArrayList<>(0));
        }

        final CriteriaQuery<Long> idQuery = criteriaBuilder.createQuery(Long.class);
        final Root<ClusterEntity> idQueryRoot = idQuery.from(ClusterEntity.class);
        idQuery.select(idQueryRoot.get(ClusterEntity_.id));
        idQuery.where(
            // NOTE: The problem with trying to reuse the predicate above even though they seem the same is they have
            //       different query objects. If there is a join added by the predicate function it won't be on the
            //       right object as these criteria queries are basically builders
            ClusterPredicates.find(
                idQueryRoot,
                idQuery,
                criteriaBuilder,
                name,
                statusStrings,
                tagEntities,
                minUpdateTime,
                maxUpdateTime
            )
        );

        final Sort sort = page.getSort();
        final List<Order> orders = new ArrayList<>();
        sort.iterator().forEachRemaining(
            order -> {
                if (order.isAscending()) {
                    orders.add(criteriaBuilder.asc(idQueryRoot.get(order.getProperty())));
                } else {
                    orders.add(criteriaBuilder.desc(idQueryRoot.get(order.getProperty())));
                }
            }
        );
        idQuery.orderBy(orders);

        final List<Long> clusterIds = this.entityManager
            .createQuery(idQuery)
            .setFirstResult(((Long) page.getOffset()).intValue())
            .setMaxResults(page.getPageSize())
            .getResultList();

        final CriteriaQuery<ClusterEntity> contentQuery = criteriaBuilder.createQuery(ClusterEntity.class);
        final Root<ClusterEntity> contentQueryRoot = contentQuery.from(ClusterEntity.class);
        contentQuery.select(contentQueryRoot);
        contentQuery.where(contentQueryRoot.get(ClusterEntity_.id).in(clusterIds));
        // Need to make the same order by or results won't be accurate
        contentQuery.orderBy(orders);

        final List<Cluster> clusters = this.entityManager
            .createQuery(contentQuery)
            .setHint(LOAD_GRAPH_HINT, this.entityManager.getEntityGraph(ClusterEntity.DTO_ENTITY_GRAPH))
            .getResultStream()
            .map(EntityV4DtoConverters::toV4ClusterDto)
            .collect(Collectors.toList());

        return new PageImpl<>(clusters, page, totalCount);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateCluster(
        @NotBlank final String id,
        @Valid final Cluster updateCluster
    ) throws NotFoundException, PreconditionFailedException {
        log.debug("[updateCluster] Called to update cluster {} with {}", id, updateCluster);
        if (!updateCluster.getId().equals(id)) {
            throw new PreconditionFailedException("Application id " + id + " inconsistent with id passed in.");
        }
        this.updateClusterEntity(
            this.clusterRepository
                .getClusterDto(id)
                .orElseThrow(() -> new NotFoundException("No cluster with id " + id + " exists")),
            updateCluster.getResources(),
            updateCluster.getMetadata()
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteAllClusters() throws PreconditionFailedException {
        log.debug("[deleteAllClusters] Called");
        for (final ClusterEntity entity : this.clusterRepository.findAll()) {
            this.deleteClusterEntity(entity);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteCluster(@NotBlank final String id) throws PreconditionFailedException {
        log.debug("[deleteCluster] Called for {}", id);
        final Optional<ClusterEntity> entity = this.clusterRepository.findByUniqueId(id);
        if (entity.isEmpty()) {
            // There's nothing to do as the caller wants to delete something that doesn't exist.
            return;
        }

        this.deleteClusterEntity(entity.get());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(isolation = Isolation.READ_COMMITTED)
    public long deleteUnusedClusters(
        final Set<ClusterStatus> deleteStatuses,
        final Instant clusterCreatedThreshold,
        final int batchSize
    ) {
        log.info(
            "[deleteUnusedClusters] Deleting with statuses {} that were created before {}",
            deleteStatuses,
            clusterCreatedThreshold
        );
        return this.clusterRepository.deleteByIdIn(
            this.clusterRepository.findUnusedClusters(
                deleteStatuses.stream().map(Enum::name).collect(Collectors.toSet()),
                clusterCreatedThreshold,
                batchSize
            )
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<Cluster> findClustersMatchingCriterion(
        @Valid final Criterion criterion,
        final boolean addDefaultStatus
    ) {
        final Criterion finalCriterion;
        if (addDefaultStatus && criterion.getStatus().isEmpty()) {
            finalCriterion = new Criterion(criterion, ClusterStatus.UP.name());
        } else {
            finalCriterion = criterion;
        }
        log.debug("[findClustersMatchingCriterion] Called to find clusters matching {}", finalCriterion);

        final CriteriaBuilder criteriaBuilder = this.entityManager.getCriteriaBuilder();
        final CriteriaQuery<ClusterEntity> criteriaQuery = criteriaBuilder.createQuery(ClusterEntity.class);
        final Root<ClusterEntity> queryRoot = criteriaQuery.from(ClusterEntity.class);
        criteriaQuery.where(
            ClusterPredicates.findClustersMatchingCriterion(queryRoot, criteriaQuery, criteriaBuilder, finalCriterion)
        );

        return this.entityManager.createQuery(criteriaQuery)
            .setHint(LOAD_GRAPH_HINT, this.entityManager.getEntityGraph(ClusterEntity.DTO_ENTITY_GRAPH))
            .getResultStream()
            .map(EntityV4DtoConverters::toV4ClusterDto)
            .collect(Collectors.toSet());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<Cluster> findClustersMatchingAnyCriterion(
        @NotEmpty final Set<@Valid Criterion> criteria,
        final boolean addDefaultStatus
    ) {
        final Set<Criterion> finalCriteria;
        if (addDefaultStatus) {
            final String defaultStatus = ClusterStatus.UP.name();
            final ImmutableSet.Builder<Criterion> criteriaBuilder = ImmutableSet.builder();
            for (final Criterion criterion : criteria) {
                if (criterion.getStatus().isPresent()) {
                    criteriaBuilder.add(criterion);
                } else {
                    criteriaBuilder.add(new Criterion(criterion, defaultStatus));
                }
            }
            finalCriteria = criteriaBuilder.build();
        } else {
            finalCriteria = criteria;
        }

        log.debug("[findClustersMatchingAnyCriterion] Called to find clusters matching any of {}", finalCriteria);

        final CriteriaBuilder criteriaBuilder = this.entityManager.getCriteriaBuilder();
        final CriteriaQuery<ClusterEntity> criteriaQuery = criteriaBuilder.createQuery(ClusterEntity.class);
        final Root<ClusterEntity> queryRoot = criteriaQuery.from(ClusterEntity.class);
        criteriaQuery.where(
            ClusterPredicates.findClustersMatchingAnyCriterion(queryRoot, criteriaQuery, criteriaBuilder, finalCriteria)
        );

        return this.entityManager.createQuery(criteriaQuery)
            .setHint(LOAD_GRAPH_HINT, this.entityManager.getEntityGraph(ClusterEntity.DTO_ENTITY_GRAPH))
            .getResultStream()
            .map(EntityV4DtoConverters::toV4ClusterDto)
            .collect(Collectors.toSet());
    }
    //endregion

    //region Command APIs

    /**
     * {@inheritDoc}
     */
    @Override
    public String saveCommand(@Valid final CommandRequest commandRequest) throws IdAlreadyExistsException {
        log.debug("[saveCommand] Called to save {}", commandRequest);
        final CommandEntity entity = new CommandEntity();
        this.setUniqueId(entity, commandRequest.getRequestedId().orElse(null));
        this.updateCommandEntity(
            entity,
            commandRequest.getResources(),
            commandRequest.getMetadata(),
            commandRequest.getExecutable(),
            commandRequest.getComputeResources().orElse(null),
            commandRequest.getClusterCriteria(),
            commandRequest.getImages()
        );

        try {
            return this.commandRepository.save(entity).getUniqueId();
        } catch (final DataIntegrityViolationException e) {
            throw new IdAlreadyExistsException(
                "A command with id " + entity.getUniqueId() + " already exists",
                e
            );
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Command getCommand(@NotBlank final String id) throws NotFoundException {
        log.debug("[getCommand] Called for {}", id);
        return EntityV4DtoConverters.toV4CommandDto(
            this.commandRepository
                .getCommandDto(id)
                .orElseThrow(() -> new NotFoundException("No command with id " + id + " exists"))
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public Page<Command> findCommands(
        @Nullable final String name,
        @Nullable final String user,
        @Nullable final Set<CommandStatus> statuses,
        @Nullable final Set<String> tags,
        final Pageable page
    ) {
        /*
         * NOTE: This is implemented this way for a reason:
         * 1. To solve the JPA N+1 problem: https://vladmihalcea.com/n-plus-1-query-problem/
         * 2. To address this: https://vladmihalcea.com/fix-hibernate-hhh000104-entity-fetch-pagination-warning-message/
         * This reduces the number of queries from potentially 100's to 3
         */
        log.debug(
            "[findCommands] Called with name = {}, user = {}, statuses = {}, tags = {}",
            name,
            user,
            statuses,
            tags
        );
        final Set<String> statusStrings = statuses != null
            ? statuses.stream().map(Enum::name).collect(Collectors.toSet())
            : null;

        // TODO: Still more optimization that can be done here to not load these entities
        //       Figure out how to use just strings in the predicate
        final Set<TagEntity> tagEntities = tags == null
            ? null
            : this.tagRepository.findByTagIn(tags);
        if (tagEntities != null && tagEntities.size() != tags.size()) {
            // short circuit for no results as at least one of the expected tags doesn't exist
            return new PageImpl<>(new ArrayList<>(0));
        }

        final CriteriaBuilder criteriaBuilder = this.entityManager.getCriteriaBuilder();
        final CriteriaQuery<Long> countQuery = criteriaBuilder.createQuery(Long.class);
        final Root<CommandEntity> countQueryRoot = countQuery.from(CommandEntity.class);
        final Subquery<Long> countIdSubQuery = countQuery.subquery(Long.class);
        final Root<CommandEntity> countIdSubQueryRoot = countIdSubQuery.from(CommandEntity.class);
        countIdSubQuery.select(countIdSubQueryRoot.get(CommandEntity_.id));
        countIdSubQuery.where(
            CommandPredicates.find(
                countIdSubQueryRoot,
                countIdSubQuery,
                criteriaBuilder,
                name,
                user,
                statusStrings,
                tagEntities
            )
        );
        countQuery.select(criteriaBuilder.count(countQueryRoot));
        countQuery.where(countQueryRoot.get(CommandEntity_.id).in(countIdSubQuery));

        final Long totalCount = this.entityManager.createQuery(countQuery).getSingleResult();
        if (totalCount == null || totalCount == 0) {
            // short circuit for no results
            return new PageImpl<>(new ArrayList<>(0));
        }

        final CriteriaQuery<Long> idQuery = criteriaBuilder.createQuery(Long.class);
        final Root<CommandEntity> idQueryRoot = idQuery.from(CommandEntity.class);
        idQuery.select(idQueryRoot.get(CommandEntity_.id));
        idQuery.where(
            // NOTE: The problem with trying to reuse the predicate above even though they seem the same is they have
            //       different query objects. If there is a join added by the predicate function it won't be on the
            //       right object as these criteria queries are basically builders
            CommandPredicates.find(
                idQueryRoot,
                idQuery,
                criteriaBuilder,
                name,
                user,
                statusStrings,
                tagEntities
            )
        );

        final Sort sort = page.getSort();
        final List<Order> orders = new ArrayList<>();
        sort.iterator().forEachRemaining(
            order -> {
                if (order.isAscending()) {
                    orders.add(criteriaBuilder.asc(idQueryRoot.get(order.getProperty())));
                } else {
                    orders.add(criteriaBuilder.desc(idQueryRoot.get(order.getProperty())));
                }
            }
        );
        idQuery.orderBy(orders);

        final List<Long> commandIds = this.entityManager
            .createQuery(idQuery)
            .setFirstResult(((Long) page.getOffset()).intValue())
            .setMaxResults(page.getPageSize())
            .getResultList();

        final CriteriaQuery<CommandEntity> contentQuery = criteriaBuilder.createQuery(CommandEntity.class);
        final Root<CommandEntity> contentQueryRoot = contentQuery.from(CommandEntity.class);
        contentQuery.select(contentQueryRoot);
        contentQuery.where(contentQueryRoot.get(CommandEntity_.id).in(commandIds));
        // Need to make the same order by or results won't be accurate
        contentQuery.orderBy(orders);

        final List<Command> commands = this.entityManager
            .createQuery(contentQuery)
            .setHint(LOAD_GRAPH_HINT, this.entityManager.getEntityGraph(CommandEntity.DTO_ENTITY_GRAPH))
            .getResultStream()
            .map(EntityV4DtoConverters::toV4CommandDto)
            .collect(Collectors.toList());

        return new PageImpl<>(commands, page, totalCount);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateCommand(
        @NotBlank final String id,
        @Valid final Command updateCommand
    ) throws NotFoundException, PreconditionFailedException {
        log.debug("[updateCommand] Called to update command {} with {}", id, updateCommand);
        if (!updateCommand.getId().equals(id)) {
            throw new PreconditionFailedException("Command id " + id + " inconsistent with id passed in.");
        }
        this.updateCommandEntity(
            this.commandRepository
                .getCommandDto(id)
                .orElseThrow(() -> new NotFoundException("No command with id " + id + " exists")),
            updateCommand.getResources(),
            updateCommand.getMetadata(),
            updateCommand.getExecutable(),
            updateCommand.getComputeResources(),
            updateCommand.getClusterCriteria(),
            updateCommand.getImages()
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteAllCommands() throws PreconditionFailedException {
        log.debug("[deleteAllCommands] Called");
        this.commandRepository.findAll().forEach(this::deleteCommandEntity);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteCommand(@NotBlank final String id) throws NotFoundException {
        log.debug("[deleteCommand] Called to delete command with id {}", id);
        this.deleteCommandEntity(
            this.commandRepository
                .getCommandAndApplications(id)
                .orElseThrow(() -> new NotFoundException("No command with id " + id + " exists"))
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addApplicationsForCommand(
        @NotBlank final String id,
        @NotEmpty final List<@NotBlank String> applicationIds
    ) throws NotFoundException, PreconditionFailedException {
        log.debug("[addApplicationsForCommand] Called to add {} to {}", applicationIds, id);
        final CommandEntity commandEntity = this.commandRepository
            .getCommandAndApplications(id)
            .orElseThrow(() -> new NotFoundException("No command with id " + id + " exists"));
        for (final String applicationId : applicationIds) {
            commandEntity.addApplication(
                this.applicationRepository
                    .getApplicationAndCommands(applicationId)
                    .orElseThrow(() -> new NotFoundException("No application with id " + applicationId + " exists"))
            );
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setApplicationsForCommand(
        @NotBlank final String id,
        @NotNull final List<@NotBlank String> applicationIds
    ) throws NotFoundException, PreconditionFailedException {
        log.debug("[setApplicationsForCommand] Called to set {} for {}", applicationIds, id);
        if (Sets.newHashSet(applicationIds).size() != applicationIds.size()) {
            throw new PreconditionFailedException("Duplicate application id in " + applicationIds);
        }
        final CommandEntity commandEntity = this.commandRepository
            .getCommandAndApplications(id)
            .orElseThrow(() -> new NotFoundException("No command with id " + id + " exists"));
        final List<ApplicationEntity> applicationEntities = Lists.newArrayList();
        for (final String applicationId : applicationIds) {
            applicationEntities.add(
                this.applicationRepository
                    .getApplicationAndCommands(applicationId)
                    .orElseThrow(() -> new NotFoundException("No application with id " + applicationId + " exists"))
            );
        }
        commandEntity.setApplications(applicationEntities);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Application> getApplicationsForCommand(final String id) throws NotFoundException {
        log.debug("[getApplicationsForCommand] Called for {}", id);
        return this.commandRepository
            .getCommandAndApplicationsDto(id)
            .orElseThrow(() -> new NotFoundException("No command with id " + id + " exists"))
            .getApplications()
            .stream()
            .map(EntityV4DtoConverters::toV4ApplicationDto)
            .collect(Collectors.toList());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeApplicationsForCommand(
        @NotBlank final String id
    ) throws NotFoundException, PreconditionFailedException {
        log.debug("[removeApplicationsForCommand] Called to for {}", id);
        this.commandRepository
            .getCommandAndApplications(id)
            .orElseThrow(() -> new NotFoundException("No command with id " + id + " exists"))
            .setApplications(null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeApplicationForCommand(
        @NotBlank final String id,
        @NotBlank final String appId
    ) throws NotFoundException {
        log.debug("[removeApplicationForCommand] Called to for {} from {}", appId, id);
        this.commandRepository
            .getCommandAndApplications(id)
            .orElseThrow(() -> new NotFoundException("No command with id " + id + " exists"))
            .removeApplication(
                this.applicationRepository
                    .getApplicationAndCommands(appId)
                    .orElseThrow(() -> new NotFoundException("No application with id " + appId + " exists"))
            );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<Cluster> getClustersForCommand(
        @NotBlank final String id,
        @Nullable final Set<ClusterStatus> statuses
    ) throws NotFoundException {
        log.debug("[getClustersForCommand] Called for {} with statuses {}", id, statuses);
        final List<Criterion> clusterCriteria = this.getClusterCriteriaForCommand(id);
        return this
            .findClustersMatchingAnyCriterion(Sets.newHashSet(clusterCriteria), false)
            .stream()
            .filter(cluster -> statuses == null || statuses.contains(cluster.getMetadata().getStatus()))
            .collect(Collectors.toSet());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Criterion> getClusterCriteriaForCommand(final String id) throws NotFoundException {
        log.debug("[getClusterCriteriaForCommand] Called to get cluster criteria for command {}", id);
        return this.commandRepository
            .getCommandAndClusterCriteria(id)
            .orElseThrow(() -> new NotFoundException("No command with id " + id + " exists"))
            .getClusterCriteria()
            .stream()
            .map(EntityV4DtoConverters::toCriterionDto)
            .collect(Collectors.toList());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addClusterCriterionForCommand(
        final String id,
        @Valid final Criterion criterion
    ) throws NotFoundException {
        log.debug("[addClusterCriterionForCommand] Called to add cluster criteria {} for command {}", criterion, id);
        this.commandRepository
            .getCommandAndClusterCriteria(id)
            .orElseThrow(() -> new NotFoundException("No command with id " + id + " exists"))
            .addClusterCriterion(this.toCriterionEntity(criterion));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addClusterCriterionForCommand(
        final String id,
        @Valid final Criterion criterion,
        @Min(0) final int priority
    ) throws NotFoundException {
        log.debug(
            "[addClusterCriterionForCommand] Called to add cluster criteria {} for command {} at priority {}",
            criterion,
            id,
            priority
        );
        this.commandRepository
            .getCommandAndClusterCriteria(id)
            .orElseThrow(() -> new NotFoundException("No command with id " + id + " exists"))
            .addClusterCriterion(this.toCriterionEntity(criterion), priority);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setClusterCriteriaForCommand(
        final String id,
        final List<@Valid Criterion> clusterCriteria
    ) throws NotFoundException {
        log.debug(
            "[setClusterCriteriaForCommand] Called to set cluster criteria {} for command {}",
            clusterCriteria,
            id
        );
        final CommandEntity commandEntity = this.commandRepository
            .getCommandAndClusterCriteria(id)
            .orElseThrow(() -> new NotFoundException("No command with id " + id + " exists"));
        this.updateClusterCriteria(commandEntity, clusterCriteria);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeClusterCriterionForCommand(final String id, @Min(0) final int priority) throws NotFoundException {
        log.debug(
            "[removeClusterCriterionForCommand] Called to remove cluster criterion with priority {} from command {}",
            priority,
            id
        );
        final CommandEntity commandEntity = this.commandRepository
            .getCommandAndClusterCriteria(id)
            .orElseThrow(() -> new NotFoundException("No command with id " + id + " exists"));
        if (priority >= commandEntity.getClusterCriteria().size()) {
            throw new NotFoundException(
                "No criterion with priority " + priority + " exists for command " + id + ". Unable to remove."
            );
        }
        try {
            final CriterionEntity criterionEntity = commandEntity.removeClusterCriterion(priority);
            log.debug("Successfully removed cluster criterion {} from command {}", criterionEntity, id);
            // Ensure this dangling criterion is deleted from the database
            this.criterionRepository.delete(criterionEntity);
        } catch (final IllegalArgumentException e) {
            log.error("Failed to remove cluster criterion with priority {} from command {}", priority, id, e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeAllClusterCriteriaForCommand(final String id) throws NotFoundException {
        log.debug("[removeAllClusterCriteriaForCommand] Called to remove all cluster criteria from command {}", id);
        this.deleteAllClusterCriteria(
            this.commandRepository
                .getCommandAndClusterCriteria(id)
                .orElseThrow(() -> new NotFoundException("No command with id " + id + " exists"))
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<Command> findCommandsMatchingCriterion(
        @Valid final Criterion criterion,
        final boolean addDefaultStatus
    ) {
        final Criterion finalCriterion;
        if (addDefaultStatus && criterion.getStatus().isEmpty()) {
            finalCriterion = new Criterion(criterion, CommandStatus.ACTIVE.name());
        } else {
            finalCriterion = criterion;
        }
        log.debug("[findCommandsMatchingCriterion] Called to find commands matching {}", finalCriterion);

        final CriteriaBuilder criteriaBuilder = this.entityManager.getCriteriaBuilder();
        final CriteriaQuery<CommandEntity> criteriaQuery = criteriaBuilder.createQuery(CommandEntity.class);
        final Root<CommandEntity> queryRoot = criteriaQuery.from(CommandEntity.class);
        criteriaQuery.where(
            CommandPredicates.findCommandsMatchingCriterion(queryRoot, criteriaQuery, criteriaBuilder, finalCriterion)
        );

        return this.entityManager.createQuery(criteriaQuery)
            .setHint(LOAD_GRAPH_HINT, this.entityManager.getEntityGraph(CommandEntity.DTO_ENTITY_GRAPH))
            .getResultStream()
            .map(EntityV4DtoConverters::toV4CommandDto)
            .collect(Collectors.toSet());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(isolation = Isolation.READ_COMMITTED)
    public int updateStatusForUnusedCommands(
        final CommandStatus desiredStatus,
        final Instant commandCreatedThreshold,
        final Set<CommandStatus> currentStatuses,
        final int batchSize
    ) {
        log.info(
            "Attempting to update at most {} commands with statuses {} "
                + "which were created before {} and haven't been used in jobs to new status {}",
            batchSize,
            currentStatuses,
            commandCreatedThreshold,
            desiredStatus
        );
        final int updateCount = this.commandRepository.setStatusWhereIdIn(
            desiredStatus.name(),
            this.commandRepository.findUnusedCommandsByStatusesCreatedBefore(
                currentStatuses.stream().map(Enum::name).collect(Collectors.toSet()),
                commandCreatedThreshold,
                batchSize
            )
        );
        log.info(
            "Updated {} commands with statuses {} "
                + "which were created before {} and haven't been used in any jobs to new status {}",
            updateCount,
            currentStatuses,
            commandCreatedThreshold,
            desiredStatus
        );
        return updateCount;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(isolation = Isolation.READ_COMMITTED)
    public long deleteUnusedCommands(
        final Set<CommandStatus> deleteStatuses,
        final Instant commandCreatedThreshold,
        final int batchSize
    ) {
        log.info(
            "Deleting commands with statuses {} that were created before {}",
            deleteStatuses,
            commandCreatedThreshold
        );
        return this.commandRepository.deleteByIdIn(
            this.commandRepository.findUnusedCommandsByStatusesCreatedBefore(
                deleteStatuses.stream().map(Enum::name).collect(Collectors.toSet()),
                commandCreatedThreshold,
                batchSize
            )
        );
    }
    //endregion

    //region Job APIs

    //region V3 Job APIs

    /**
     * {@inheritDoc}
     */
    @Override
    public Job getJob(@NotBlank final String id) throws GenieException {
        log.debug("[getJob] Called with id {}", id);
        return EntityV3DtoConverters.toJobDto(
            this.jobRepository
                .getV3Job(id)
                .orElseThrow(() -> new GenieNotFoundException("No job with id " + id))
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JobExecution getJobExecution(@NotBlank final String id) throws GenieException {
        log.debug("[getJobExecution] Called with id {}", id);
        return EntityV3DtoConverters.toJobExecutionDto(
            this.jobRepository
                .findByUniqueId(id, JobExecutionProjection.class)
                .orElseThrow(() -> new GenieNotFoundException("No job with id " + id))
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public com.netflix.genie.common.dto.JobMetadata getJobMetadata(@NotBlank final String id) throws GenieException {
        log.debug("[getJobMetadata] Called with id {}", id);
        return EntityV3DtoConverters.toJobMetadataDto(
            this.jobRepository
                .findByUniqueId(id, JobMetadataProjection.class)
                .orElseThrow(() -> new GenieNotFoundException("No job found for id " + id))
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    @SuppressWarnings("checkstyle:parameternumber")
    public Page<JobSearchResult> findJobs(
        @Nullable final String id,
        @Nullable final String name,
        @Nullable final String user,
        @Nullable final Set<com.netflix.genie.common.dto.JobStatus> statuses,
        @Nullable final Set<String> tags,
        @Nullable final String clusterName,
        @Nullable final String clusterId,
        @Nullable final String commandName,
        @Nullable final String commandId,
        @Nullable final Instant minStarted,
        @Nullable final Instant maxStarted,
        @Nullable final Instant minFinished,
        @Nullable final Instant maxFinished,
        @Nullable final String grouping,
        @Nullable final String groupingInstance,
        @NotNull final Pageable page
    ) {
        log.debug("[findJobs] Called");

        ClusterEntity clusterEntity = null;
        if (clusterId != null) {
            final Optional<ClusterEntity> optionalClusterEntity
                = this.getEntityOrNullForFindJobs(this.clusterRepository, clusterId, clusterName);
            if (optionalClusterEntity.isPresent()) {
                clusterEntity = optionalClusterEntity.get();
            } else {
                // Won't find anything matching the query
                return new PageImpl<>(Lists.newArrayList(), page, 0);
            }
        }
        CommandEntity commandEntity = null;
        if (commandId != null) {
            final Optional<CommandEntity> optionalCommandEntity
                = this.getEntityOrNullForFindJobs(this.commandRepository, commandId, commandName);
            if (optionalCommandEntity.isPresent()) {
                commandEntity = optionalCommandEntity.get();
            } else {
                // Won't find anything matching the query
                return new PageImpl<>(Lists.newArrayList(), page, 0);
            }
        }

        final Set<String> statusStrings = statuses != null
            ? statuses.stream().map(Enum::name).collect(Collectors.toSet())
            : null;

        final CriteriaBuilder cb = this.entityManager.getCriteriaBuilder();
        final CriteriaQuery<Long> countQuery = cb.createQuery(Long.class);
        final Root<JobEntity> root = countQuery.from(JobEntity.class);

        countQuery
            .select(cb.count(root))
            .where(
                JobPredicates
                    .getFindPredicate(
                        root,
                        cb,
                        id,
                        name,
                        user,
                        statusStrings,
                        tags,
                        clusterName,
                        clusterEntity,
                        commandName,
                        commandEntity,
                        minStarted,
                        maxStarted,
                        minFinished,
                        maxFinished,
                        grouping,
                        groupingInstance
                    )
            );

        final long totalCount = this.entityManager.createQuery(countQuery).getSingleResult();
        if (totalCount == 0) {
            // short circuit for no results
            return new PageImpl<>(new ArrayList<>(0));
        }

        final CriteriaQuery<JobSearchResult> contentQuery = cb.createQuery(JobSearchResult.class);
        final Root<JobEntity> contentQueryRoot = contentQuery.from(JobEntity.class);

        contentQuery.multiselect(
            contentQueryRoot.get(JobEntity_.uniqueId),
            contentQueryRoot.get(JobEntity_.name),
            contentQueryRoot.get(JobEntity_.user),
            contentQueryRoot.get(JobEntity_.status),
            contentQueryRoot.get(JobEntity_.started),
            contentQueryRoot.get(JobEntity_.finished),
            contentQueryRoot.get(JobEntity_.clusterName),
            contentQueryRoot.get(JobEntity_.commandName)
        );

        contentQuery.where(
            JobPredicates
                .getFindPredicate(
                    contentQueryRoot,
                    cb,
                    id,
                    name,
                    user,
                    statusStrings,
                    tags,
                    clusterName,
                    clusterEntity,
                    commandName,
                    commandEntity,
                    minStarted,
                    maxStarted,
                    minFinished,
                    maxFinished,
                    grouping,
                    groupingInstance
                )
        );

        final Sort sort = page.getSort();
        final List<Order> orders = new ArrayList<>();
        sort.iterator().forEachRemaining(
            order -> {
                if (order.isAscending()) {
                    orders.add(cb.asc(root.get(order.getProperty())));
                } else {
                    orders.add(cb.desc(root.get(order.getProperty())));
                }
            }
        );
        contentQuery.orderBy(orders);

        final List<JobSearchResult> results = this.entityManager
            .createQuery(contentQuery)
            .setFirstResult(((Long) page.getOffset()).intValue())
            .setMaxResults(page.getPageSize())
            .getResultList();

        return new PageImpl<>(results, page, totalCount);
    }
    //endregion

    //region V4 Job APIs

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(isolation = Isolation.READ_COMMITTED)
    public long deleteJobsCreatedBefore(
        @NotNull final Instant creationThreshold,
        @NotNull final Set<JobStatus> excludeStatuses,
        @Min(1) final int batchSize
    ) {
        final String excludeStatusesString = excludeStatuses.toString();
        final String creationThresholdString = creationThreshold.toString();
        log.info(
            "[deleteJobsCreatedBefore] Attempting to delete at most {} jobs created before {} that do not have any of "
                + "these statuses {}",
            batchSize,
            creationThresholdString,
            excludeStatusesString
        );
        final Set<String> ignoredStatusStrings = excludeStatuses.stream().map(Enum::name).collect(Collectors.toSet());
        final long numJobsDeleted = this.jobRepository.deleteByIdIn(
            this.jobRepository.findJobsCreatedBefore(
                creationThreshold,
                ignoredStatusStrings,
                batchSize
            )
        );
        log.info(
            "[deleteJobsCreatedBefore] Deleted {} jobs created before {} that did not have any of these statuses {}",
            numJobsDeleted,
            creationThresholdString,
            excludeStatusesString
        );
        return numJobsDeleted;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    public String saveJobSubmission(@Valid final JobSubmission jobSubmission) throws IdAlreadyExistsException {
        log.debug("[saveJobSubmission] Attempting to save job submission {}", jobSubmission);
        // TODO: Metrics
        final JobEntity jobEntity = new JobEntity();
        jobEntity.setStatus(JobStatus.RESERVED.name());

        final JobRequest jobRequest = jobSubmission.getJobRequest();
        final JobRequestMetadata jobRequestMetadata = jobSubmission.getJobRequestMetadata();

        // Create the unique id if one doesn't already exist
        this.setUniqueId(jobEntity, jobRequest.getRequestedId().orElse(null));

        jobEntity.setCommandArgs(jobRequest.getCommandArgs());

        this.setJobMetadataFields(
            jobEntity,
            jobRequest.getMetadata(),
            jobRequest.getResources().getSetupFile().orElse(null)
        );
        this.setJobExecutionEnvironmentFields(jobEntity, jobRequest.getResources(), jobSubmission.getAttachments());
        this.setExecutionResourceCriteriaFields(jobEntity, jobRequest.getCriteria());
        this.setRequestedJobEnvironmentFields(jobEntity, jobRequest.getRequestedJobEnvironment());
        this.setRequestedAgentConfigFields(jobEntity, jobRequest.getRequestedAgentConfig());
        this.setRequestMetadataFields(jobEntity, jobRequestMetadata);

        // Set archive status
        jobEntity.setArchiveStatus(
            jobRequest.getRequestedAgentConfig().isArchivingDisabled()
                ? ArchiveStatus.DISABLED.name()
                : ArchiveStatus.PENDING.name()
        );

        // Persist. Catch exception if the ID is reused
        try {
            final String id = this.jobRepository.save(jobEntity).getUniqueId();
            log.debug(
                "[saveJobSubmission] Saved job submission {} under job id {}",
                jobSubmission,
                id
            );
            final SpanCustomizer spanCustomizer = this.addJobIdTag(id);
            // This is a new job so add flag representing that fact
            this.tagAdapter.tag(spanCustomizer, TracingConstants.NEW_JOB_TAG, TracingConstants.TRUE_VALUE);
            return id;
        } catch (final DataIntegrityViolationException e) {
            throw new IdAlreadyExistsException(
                "A job with id " + jobEntity.getUniqueId() + " already exists. Unable to reserve id.",
                e
            );
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JobRequest getJobRequest(@NotBlank final String id) throws NotFoundException {
        log.debug("[getJobRequest] Requested for id {}", id);
        return this.jobRepository
            .getV4JobRequest(id)
            .map(EntityV4DtoConverters::toV4JobRequestDto)
            .orElseThrow(() -> new NotFoundException("No job ith id " + id + " exists"));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void saveResolvedJob(
        @NotBlank final String id,
        @Valid final ResolvedJob resolvedJob
    ) throws NotFoundException {
        log.debug("[saveResolvedJob] Requested to save resolved information {} for job with id {}", resolvedJob, id);
        final JobEntity entity = this.getJobEntity(id);

        try {
            if (entity.isResolved()) {
                log.error("[saveResolvedJob] Job {} was already resolved", id);
                // This job has already been resolved there's nothing further to save
                return;
            }
            // Make sure if the job is resolvable otherwise don't do anything
            if (!DtoConverters.toV4JobStatus(entity.getStatus()).isResolvable()) {
                log.error(
                    "[saveResolvedJob] Job {} is already in a non-resolvable state {}. Needs to be one of {}. Won't "
                        + "save resolved info",
                    id,
                    entity.getStatus(),
                    JobStatus.getResolvableStatuses()
                );
                return;
            }
            final JobSpecification jobSpecification = resolvedJob.getJobSpecification();
            this.setExecutionResources(
                entity,
                jobSpecification.getCluster().getId(),
                jobSpecification.getCommand().getId(),
                jobSpecification
                    .getApplications()
                    .stream()
                    .map(JobSpecification.ExecutionResource::getId)
                    .collect(Collectors.toList())
            );

            entity.setEnvironmentVariables(jobSpecification.getEnvironmentVariables());
            entity.setJobDirectoryLocation(jobSpecification.getJobDirectoryLocation().getAbsolutePath());
            jobSpecification.getArchiveLocation().ifPresent(entity::setArchiveLocation);
            jobSpecification.getTimeout().ifPresent(entity::setTimeoutUsed);

            final JobEnvironment jobEnvironment = resolvedJob.getJobEnvironment();
            this.updateComputeResources(
                jobEnvironment.getComputeResources(),
                entity::setCpuUsed,
                entity::setGpuUsed,
                entity::setMemoryUsed,
                entity::setDiskMbUsed,
                entity::setNetworkMbpsUsed
            );
            this.updateImages(
                jobEnvironment.getImages(),
                entity::setImagesUsed
            );

            entity.setResolved(true);
            entity.setStatus(JobStatus.RESOLVED.name());
            log.debug("[saveResolvedJob] Saved resolved information {} for job with id {}", resolvedJob, id);
        } catch (final NotFoundException e) {
            log.error(
                "[saveResolvedJob] Unable to save resolved job information {} for job {} due to {}",
                resolvedJob,
                id,
                e.getMessage(),
                e
            );
            throw e;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<JobSpecification> getJobSpecification(@NotBlank final String id) throws NotFoundException {
        log.debug("[getJobSpecification] Requested to get job specification for job {}", id);
        final JobSpecificationProjection projection = this.jobRepository
            .getJobSpecification(id)
            .orElseThrow(
                () -> new NotFoundException("No job ith id " + id + " exists. Unable to get job specification.")
            );

        return projection.isResolved()
            ? Optional.of(EntityV4DtoConverters.toJobSpecificationDto(projection))
            : Optional.empty();
    }

    /**
     * {@inheritDoc}
     */
    // TODO: The AOP aspects are firing on a lot of these APIs for retries and we may not want them to given a lot of
    //       these are un-recoverable. May want to revisit what is in the aspect.
    @Override
    public void claimJob(
        @NotBlank final String id,
        @Valid final AgentClientMetadata agentClientMetadata
    ) throws NotFoundException, GenieJobAlreadyClaimedException, GenieInvalidStatusException {
        log.debug("[claimJob] Agent with metadata {} requesting to claim job with id {}", agentClientMetadata, id);
        final JobEntity jobEntity = this.getJobEntity(id);

        if (jobEntity.isClaimed()) {
            throw new GenieJobAlreadyClaimedException("Job with id " + id + " is already claimed. Unable to claim.");
        }

        final JobStatus currentStatus = DtoConverters.toV4JobStatus(jobEntity.getStatus());
        // The job must be in one of the claimable states in order to be claimed
        // TODO: Perhaps could use jobEntity.isResolved here also but wouldn't check the case that the job was in a
        //       terminal state like killed or invalid in which case we shouldn't claim it anyway as the agent would
        //       continue running
        if (!currentStatus.isClaimable()) {
            throw new GenieInvalidStatusException(
                "Job "
                    + id
                    + " is in status "
                    + currentStatus
                    + " and can't be claimed. Needs to be one of "
                    + JobStatus.getClaimableStatuses()
            );
        }

        // Good to claim
        jobEntity.setClaimed(true);
        jobEntity.setStatus(JobStatus.CLAIMED.name());
        // TODO: It might be nice to set the status message as well to something like "Job claimed by XYZ..."
        //       we could do this in other places too like after reservation, resolving, etc

        // TODO: Should these be required? We're reusing the DTO here but perhaps the expectation at this point
        //       is that the agent will always send back certain metadata
        agentClientMetadata.getHostname().ifPresent(jobEntity::setAgentHostname);
        agentClientMetadata.getVersion().ifPresent(jobEntity::setAgentVersion);
        agentClientMetadata.getPid().ifPresent(jobEntity::setAgentPid);
        log.debug("[claimJob] Claimed job {} for agent with metadata {}", id, agentClientMetadata);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JobStatus updateJobStatus(
        @NotBlank final String id,
        @NotNull final JobStatus currentStatus,
        @NotNull final JobStatus newStatus,
        @Nullable final String newStatusMessage
    ) throws NotFoundException {
        log.debug(
            "[updateJobStatus] Requested to change the status of job {} from {} to {} with message {}",
            id,
            currentStatus,
            newStatus,
            newStatusMessage
        );
        if (currentStatus == newStatus) {
            log.debug(
                "[updateJobStatus] Requested new status for {} is same as current status: {}. Skipping update.",
                id,
                currentStatus
            );
            return newStatus;
        }

        final JobEntity jobEntity = this.getJobEntity(id);

        final JobStatus actualCurrentStatus = DtoConverters.toV4JobStatus(jobEntity.getStatus());
        if (actualCurrentStatus != currentStatus) {
            log.warn(
                "[updateJobStatus] Job {} actual status {} differs from expected status {}. Skipping update.",
                id,
                actualCurrentStatus,
                currentStatus
            );
            return actualCurrentStatus;
        }

        // TODO: Should we prevent updating status for statuses already covered by "reserveJobId" and
        //      "saveResolvedJob"?

        // Only change the status if the entity isn't already in a terminal state
        if (actualCurrentStatus.isActive()) {
            jobEntity.setStatus(newStatus.name());
            jobEntity.setStatusMsg(StringUtils.truncate(newStatusMessage, MAX_STATUS_MESSAGE_LENGTH));

            if (newStatus.equals(JobStatus.RUNNING)) {
                // Status being changed to running so set start date.
                jobEntity.setStarted(Instant.now());
            } else if (jobEntity.getStarted().isPresent() && newStatus.isFinished()) {
                // Since start date is set the job was running previously and now has finished
                // with status killed, failed or succeeded. So we set the job finish time.
                jobEntity.setFinished(Instant.now());
            }

            log.debug(
                "[updateJobStatus] Changed the status of job {} from {} to {} with message {}",
                id,
                currentStatus,
                newStatus,
                newStatusMessage
            );

            return newStatus;
        } else {
            log.warn(
                "[updateJobStatus] Job status for {} is already terminal state {}. Skipping update.",
                id,
                actualCurrentStatus
            );
            return actualCurrentStatus;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateJobArchiveStatus(
        @NotBlank(message = "No job id entered. Unable to update.") final String id,
        @NotNull(message = "Status cannot be null.") final ArchiveStatus archiveStatus
    ) throws NotFoundException {
        log.debug(
            "[updateJobArchiveStatus] Requested to change the archive status of job {} to {}",
            id,
            archiveStatus
        );

        this.jobRepository
            .findByUniqueId(id)
            .orElseThrow(() -> new NotFoundException("No job exists for the id specified"))
            .setArchiveStatus(archiveStatus.name());

        log.debug(
            "[updateJobArchiveStatus] Changed the archive status of job {} to {}",
            id,
            archiveStatus
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JobStatus getJobStatus(@NotBlank final String id) throws NotFoundException {
        return DtoConverters.toV4JobStatus(
            this.jobRepository
                .getJobStatus(id)
                .orElseThrow(() -> new NotFoundException("No job with id " + id + " exists. Unable to get status."))
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ArchiveStatus getJobArchiveStatus(@NotBlank final String id) throws NotFoundException {
        try {
            return ArchiveStatus.valueOf(
                this.jobRepository
                    .getArchiveStatus(id)
                    .orElseThrow(() -> new NotFoundException("No job with id " + id + " exists"))
            );
        } catch (IllegalArgumentException e) {
            return ArchiveStatus.UNKNOWN;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<String> getJobArchiveLocation(@NotBlank final String id) throws NotFoundException {
        final CriteriaBuilder criteriaBuilder = this.entityManager.getCriteriaBuilder();
        final CriteriaQuery<String> query = criteriaBuilder.createQuery(String.class);
        final Root<JobEntity> root = query.from(JobEntity.class);
        query.select(root.get(JobEntity_.archiveLocation));
        query.where(criteriaBuilder.equal(root.get(JobEntity_.uniqueId), id));
        try {
            return Optional.ofNullable(
                this.entityManager
                    .createQuery(query)
                    .getSingleResult()
            );
        } catch (final NoResultException e) {
            throw new NotFoundException("No job with id " + id + " exits.", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FinishedJob getFinishedJob(@NotBlank final String id) throws NotFoundException, GenieInvalidStatusException {
        // TODO
        return this.jobRepository.findByUniqueId(id, FinishedJobProjection.class)
            .map(EntityV4DtoConverters::toFinishedJobDto)
            .orElseThrow(() -> new NotFoundException("No job with id " + id + " exists."));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isApiJob(@NotBlank final String id) throws NotFoundException {
        return this.jobRepository
            .isAPI(id)
            .orElseThrow(() -> new NotFoundException("No job with id " + id + " exists"));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Cluster getJobCluster(@NotBlank final String id) throws NotFoundException {
        log.debug("[getJobCluster] Called for job {}", id);
        return EntityV4DtoConverters.toV4ClusterDto(
            this.jobRepository.getJobCluster(id)
                .orElseThrow(() -> new NotFoundException("No job with id " + id + " exists"))
                .getCluster()
                .orElseThrow(() -> new NotFoundException("Job " + id + " has no associated cluster"))
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Command getJobCommand(@NotBlank final String id) throws NotFoundException {
        log.debug("[getJobCommand] Called for job {}", id);
        return EntityV4DtoConverters.toV4CommandDto(
            this.jobRepository.getJobCommand(id)
                .orElseThrow(() -> new NotFoundException("No job with id " + id + " exists"))
                .getCommand()
                .orElseThrow(() -> new NotFoundException("Job " + id + " has no associated command"))
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Application> getJobApplications(@NotBlank final String id) throws NotFoundException {
        log.debug("[getJobApplications] Called for job {}", id);
        return this.jobRepository.getJobApplications(id)
            .orElseThrow(() -> new NotFoundException("No job with id " + id + " exists"))
            .getApplications()
            .stream()
            .map(EntityV4DtoConverters::toV4ApplicationDto)
            .collect(Collectors.toList());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public long getActiveJobCountForUser(@NotBlank final String user) {
        log.debug("[getActiveJobCountForUser] Called for jobs with user {}", user);
        final Long count = this.jobRepository.countJobsByUserAndStatusIn(user, ACTIVE_STATUS_SET);
        if (count == null || count < 0) {
            throw new GenieRuntimeException("Count query for user " + user + "produced an unexpected result: " + count);
        }
        return count;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public Map<String, UserResourcesSummary> getUserResourcesSummaries(
        final Set<JobStatus> statuses,
        final boolean api
    ) {
        log.debug("[getUserResourcesSummaries] Called for statuses {} and api {}", statuses, api);
        return this.jobRepository
            .getUserJobResourcesAggregates(
                statuses.stream().map(JobStatus::name).collect(Collectors.toSet()),
                api
            )
            .stream()
            .map(EntityV3DtoConverters::toUserResourceSummaryDto)
            .collect(Collectors.toMap(UserResourcesSummary::getUser, userResourcesSummary -> userResourcesSummary));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getUsedMemoryOnHost(@NotBlank final String hostname) {
        log.debug("[getUsedMemoryOnHost] Called for hostname {}", hostname);
        return this.jobRepository.getTotalMemoryUsedOnHost(hostname, USING_MEMORY_JOB_SET);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<String> getActiveJobs() {
        log.debug("[getActiveJobs] Called");
        return this.jobRepository.getJobIdsWithStatusIn(ACTIVE_STATUS_SET);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<String> getUnclaimedJobs() {
        log.debug("[getUnclaimedJobs] Called");
        return this.jobRepository.getJobIdsWithStatusIn(UNCLAIMED_STATUS_SET);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JobInfoAggregate getHostJobInformation(@NotBlank final String hostname) {
        log.debug("[getHostJobInformation] Called for hostname {}", hostname);
        return this.jobRepository.getHostJobInfo(hostname, ACTIVE_STATUS_SET, USING_MEMORY_JOB_SET);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<String> getJobsWithStatusAndArchiveStatusUpdatedBefore(
        @NotEmpty final Set<JobStatus> statuses,
        @NotEmpty final Set<ArchiveStatus> archiveStatuses,
        @NotNull final Instant updated
    ) {
        log.debug(
            "[getJobsWithStatusAndArchiveStatusUpdatedBefore] Called with statuses {}, archiveStatuses {}, updated {}",
            statuses,
            archiveStatuses,
            updated
        );
        return this.jobRepository.getJobsWithStatusAndArchiveStatusUpdatedBefore(
            statuses.stream().map(JobStatus::name).collect(Collectors.toSet()),
            archiveStatuses.stream().map(ArchiveStatus::name).collect(Collectors.toSet()),
            updated
        );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void updateRequestedLauncherExt(
        @NotBlank(message = "No job id entered. Unable to update.") final String id,
        @NotNull(message = "Status cannot be null.") final JsonNode launcherExtension
    ) throws NotFoundException {
        log.debug("[updateRequestedLauncherExt] Requested to update launcher requested ext of job {}", id);

        this.jobRepository
            .findByUniqueId(id)
            .orElseThrow(() -> new NotFoundException("No job exists for the id specified"))
            .setRequestedLauncherExt(launcherExtension);

        log.debug("[updateRequestedLauncherExt] Updated launcher requested ext of job {}", id);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JsonNode getRequestedLauncherExt(@NotBlank final String id) throws NotFoundException {
        log.debug("[getRequestedLauncherExt] Requested for job {}", id);
        return this.jobRepository.getRequestedLauncherExt(id).orElse(NullNode.getInstance());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateLauncherExt(
        @NotBlank(message = "No job id entered. Unable to update.") final String id,
        @NotNull(message = "Status cannot be null.") final JsonNode launcherExtension
    ) throws NotFoundException {
        log.debug("[updateLauncherExt] Requested to update launcher ext of job {}", id);

        this.jobRepository
            .findByUniqueId(id)
            .orElseThrow(() -> new NotFoundException("No job exists for the id specified"))
            .setLauncherExt(launcherExtension);

        log.debug("[updateLauncherExt] Updated launcher ext of job {}", id);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JsonNode getLauncherExt(@NotBlank final String id) throws NotFoundException {
        log.debug("[getLauncherExt] Requested for job {}", id);
        return this.jobRepository.getLauncherExt(id).orElse(NullNode.getInstance());
    }

    //endregion
    //endregion

    //region General CommonResource APIs

    /**
     * {@inheritDoc}
     */
    @Override
    public <R extends CommonResource> void addConfigsToResource(
        @NotBlank final String id,
        final Set<@Size(max = 1024) String> configs,
        final Class<R> resourceClass
    ) throws NotFoundException {
        this.getResourceConfigEntities(id, resourceClass).addAll(this.createOrGetFileEntities(configs));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <R extends CommonResource> Set<String> getConfigsForResource(
        @NotBlank final String id,
        final Class<R> resourceClass
    ) throws NotFoundException {
        return this.getResourceConfigEntities(id, resourceClass)
            .stream()
            .map(FileEntity::getFile)
            .collect(Collectors.toSet());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <R extends CommonResource> void updateConfigsForResource(
        @NotBlank final String id,
        final Set<@Size(max = 1024) String> configs,
        final Class<R> resourceClass
    ) throws NotFoundException {
        final Set<FileEntity> configEntities = this.getResourceConfigEntities(id, resourceClass);
        configEntities.clear();
        configEntities.addAll(this.createOrGetFileEntities(configs));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <R extends CommonResource> void removeAllConfigsForResource(
        @NotBlank final String id,
        final Class<R> resourceClass
    ) throws NotFoundException {
        final Set<FileEntity> configEntities = this.getResourceConfigEntities(id, resourceClass);
        configEntities.clear();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <R extends CommonResource> void removeConfigForResource(
        @NotBlank final String id,
        @NotBlank final String config,
        final Class<R> resourceClass
    ) throws NotFoundException {
        this.getResourceConfigEntities(id, resourceClass).removeIf(entity -> config.equals(entity.getFile()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <R extends CommonResource> void addDependenciesToResource(
        @NotBlank final String id,
        final Set<@Size(max = 1024) String> dependencies,
        final Class<R> resourceClass
    ) throws NotFoundException {
        this.getResourceDependenciesEntities(id, resourceClass).addAll(this.createOrGetFileEntities(dependencies));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <R extends CommonResource> Set<String> getDependenciesForResource(
        @NotBlank final String id,
        final Class<R> resourceClass
    ) throws NotFoundException {
        return this.getResourceDependenciesEntities(id, resourceClass)
            .stream()
            .map(FileEntity::getFile)
            .collect(Collectors.toSet());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <R extends CommonResource> void updateDependenciesForResource(
        @NotBlank final String id,
        final Set<@Size(max = 1024) String> dependencies,
        final Class<R> resourceClass
    ) throws NotFoundException {
        final Set<FileEntity> dependencyEntities = this.getResourceDependenciesEntities(id, resourceClass);
        dependencyEntities.clear();
        dependencyEntities.addAll(this.createOrGetFileEntities(dependencies));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <R extends CommonResource> void removeAllDependenciesForResource(
        @NotBlank final String id,
        final Class<R> resourceClass
    ) throws NotFoundException {
        final Set<FileEntity> dependencyEntities = this.getResourceDependenciesEntities(id, resourceClass);
        dependencyEntities.clear();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <R extends CommonResource> void removeDependencyForResource(
        @NotBlank final String id,
        @NotBlank final String dependency,
        final Class<R> resourceClass
    ) throws NotFoundException {
        this.getResourceDependenciesEntities(id, resourceClass).removeIf(entity -> dependency.equals(entity.getFile()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <R extends CommonResource> void addTagsToResource(
        @NotBlank final String id,
        final Set<@Size(max = 255) String> tags,
        final Class<R> resourceClass
    ) throws NotFoundException {
        this.getResourceTagEntities(id, resourceClass).addAll(this.createOrGetTagEntities(tags));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <R extends CommonResource> Set<String> getTagsForResource(
        @NotBlank final String id,
        final Class<R> resourceClass
    ) throws NotFoundException {
        return this.getResourceTagEntities(id, resourceClass)
            .stream()
            .map(TagEntity::getTag)
            .collect(Collectors.toSet());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <R extends CommonResource> void updateTagsForResource(
        @NotBlank final String id,
        final Set<@Size(max = 255) String> tags,
        final Class<R> resourceClass
    ) throws NotFoundException {
        final Set<TagEntity> tagEntities = this.getResourceTagEntities(id, resourceClass);
        tagEntities.clear();
        tagEntities.addAll(this.createOrGetTagEntities(tags));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <R extends CommonResource> void removeAllTagsForResource(
        @NotBlank final String id,
        final Class<R> resourceClass
    ) throws NotFoundException {
        final Set<TagEntity> tagEntities = this.getResourceTagEntities(id, resourceClass);
        tagEntities.clear();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <R extends CommonResource> void removeTagForResource(
        @NotBlank final String id,
        @NotBlank final String tag,
        final Class<R> resourceClass
    ) throws NotFoundException {
        this.getResourceTagEntities(id, resourceClass).removeIf(entity -> tag.equals(entity.getTag()));
    }
    //endregion

    //region Tag APIs

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(isolation = Isolation.READ_COMMITTED)
    public long deleteUnusedTags(@NotNull final Instant createdThreshold, @Min(1) final int batchSize) {
        log.info("[deleteUnusedTags] Called to delete unused tags created before {}", createdThreshold);
        return this.tagRepository.deleteByIdIn(
            this.tagRepository
                .findUnusedTags(createdThreshold, batchSize)
                .stream()
                .map(Number::longValue)
                .collect(Collectors.toSet())
        );
    }
    //endregion

    //region File APIs

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(isolation = Isolation.READ_COMMITTED)
    public long deleteUnusedFiles(@NotNull final Instant createdThresholdLowerBound,
                                  @NotNull final Instant createdThresholdUpperBound,
                                  @Min(1) final int batchSize) {
        log.debug(
            "[deleteUnusedFiles] Called to delete unused files created between {} and {}",
            createdThresholdLowerBound,
            createdThresholdUpperBound
        );
        return this.fileRepository.deleteByIdIn(
            this.fileRepository
                .findUnusedFiles(createdThresholdLowerBound, createdThresholdUpperBound, batchSize)
                .stream()
                .map(Number::longValue)
                .collect(Collectors.toSet())
        );
    }
    //endregion

    //region Helper Methods
    private ApplicationEntity getApplicationEntity(final String id) throws NotFoundException {
        return this.applicationRepository
            .findByUniqueId(id)
            .orElseThrow(() -> new NotFoundException("No application with id " + id + " exists"));
    }

    private ClusterEntity getClusterEntity(final String id) throws NotFoundException {
        return this.clusterRepository
            .findByUniqueId(id)
            .orElseThrow(() -> new NotFoundException("No cluster with id " + id + " exists"));
    }

    private CommandEntity getCommandEntity(final String id) throws NotFoundException {
        return this.commandRepository
            .findByUniqueId(id)
            .orElseThrow(() -> new NotFoundException("No command with id " + id + " exists"));
    }

    private JobEntity getJobEntity(final String id) throws NotFoundException {
        return this.jobRepository
            .findByUniqueId(id)
            .orElseThrow(() -> new NotFoundException("No job with id " + id + " exists"));
    }

    private FileEntity createOrGetFileEntity(final String file) {
        return this.createOrGetSharedEntity(
            file,
            this.fileRepository::findByFile,
            FileEntity::new,
            this.fileRepository::saveAndFlush
        );
    }

    private Set<FileEntity> createOrGetFileEntities(final Set<String> files) {
        return files.stream().map(this::createOrGetFileEntity).collect(Collectors.toSet());
    }

    private TagEntity createOrGetTagEntity(final String tag) {
        return this.createOrGetSharedEntity(
            tag,
            this.tagRepository::findByTag,
            TagEntity::new,
            this.tagRepository::saveAndFlush
        );
    }

    private Set<TagEntity> createOrGetTagEntities(final Set<String> tags) {
        return tags.stream().map(this::createOrGetTagEntity).collect(Collectors.toSet());
    }

    private <E> E createOrGetSharedEntity(
        final String value,
        final Function<String, Optional<E>> find,
        final Function<String, E> entityCreation,
        final Function<E, E> saveAndFlush
    ) {
        final Optional<E> existingEntity = find.apply(value);
        if (existingEntity.isPresent()) {
            return existingEntity.get();
        }

        try {
            return saveAndFlush.apply(entityCreation.apply(value));
        } catch (final DataIntegrityViolationException e) {
            // If this isn't found now there's really nothing we can do so throw runtime
            return find
                .apply(value)
                .orElseThrow(
                    () -> new GenieRuntimeException(value + " entity creation failed but still can't find record", e)
                );
        }
    }

    private <R extends CommonResource> Set<FileEntity> getResourceConfigEntities(
        final String id,
        final Class<R> resourceClass
    ) throws NotFoundException {
        if (resourceClass.equals(Application.class)) {
            return this.getApplicationEntity(id).getConfigs();
        } else if (resourceClass.equals(Cluster.class)) {
            return this.getClusterEntity(id).getConfigs();
        } else if (resourceClass.equals(Command.class)) {
            return this.getCommandEntity(id).getConfigs();
        } else {
            throw new IllegalArgumentException("Unsupported type: " + resourceClass);
        }
    }

    private <R extends CommonResource> Set<FileEntity> getResourceDependenciesEntities(
        final String id,
        final Class<R> resourceClass
    ) throws NotFoundException {
        if (resourceClass.equals(Application.class)) {
            return this.getApplicationEntity(id).getDependencies();
        } else if (resourceClass.equals(Cluster.class)) {
            return this.getClusterEntity(id).getDependencies();
        } else if (resourceClass.equals(Command.class)) {
            return this.getCommandEntity(id).getDependencies();
        } else {
            throw new IllegalArgumentException("Unsupported type: " + resourceClass);
        }
    }

    private <R extends CommonResource> Set<TagEntity> getResourceTagEntities(
        final String id,
        final Class<R> resourceClass
    ) throws NotFoundException {
        if (resourceClass.equals(Application.class)) {
            return this.getApplicationEntity(id).getTags();
        } else if (resourceClass.equals(Cluster.class)) {
            return this.getClusterEntity(id).getTags();
        } else if (resourceClass.equals(Command.class)) {
            return this.getCommandEntity(id).getTags();
        } else {
            throw new IllegalArgumentException("Unsupported type: " + resourceClass);
        }
    }

    private <E extends UniqueIdEntity> void setUniqueId(final E entity, @Nullable final String requestedId) {
        if (requestedId != null) {
            entity.setUniqueId(requestedId);
            entity.setRequestedId(true);
        } else {
            entity.setUniqueId(UUID.randomUUID().toString());
            entity.setRequestedId(false);
        }
    }

    private void setEntityResources(
        final ExecutionEnvironment resources,
        final Consumer<Set<FileEntity>> configsConsumer,
        final Consumer<Set<FileEntity>> dependenciesConsumer
    ) {
        // Save all the unowned entities first to avoid unintended flushes
        configsConsumer.accept(this.createOrGetFileEntities(resources.getConfigs()));
        dependenciesConsumer.accept(this.createOrGetFileEntities(resources.getDependencies()));
    }

    private void setEntityTags(final Set<String> tags, final Consumer<Set<TagEntity>> tagsConsumer) {
        tagsConsumer.accept(this.createOrGetTagEntities(tags));
    }

    private void updateApplicationEntity(
        final ApplicationEntity entity,
        final ExecutionEnvironment resources,
        final ApplicationMetadata metadata
    ) {
        entity.setStatus(metadata.getStatus().name());
        entity.setType(metadata.getType().orElse(null));
        this.setEntityResources(resources, entity::setConfigs, entity::setDependencies);
        this.setEntityTags(metadata.getTags(), entity::setTags);
        this.setBaseEntityMetadata(entity, metadata, resources.getSetupFile().orElse(null));
    }

    private void updateClusterEntity(
        final ClusterEntity entity,
        final ExecutionEnvironment resources,
        final ClusterMetadata metadata
    ) {
        entity.setStatus(metadata.getStatus().name());
        this.setEntityResources(resources, entity::setConfigs, entity::setDependencies);
        this.setEntityTags(metadata.getTags(), entity::setTags);
        this.setBaseEntityMetadata(entity, metadata, resources.getSetupFile().orElse(null));
    }

    // Compiler keeps complaining about `executable` being marked nullable, it isn't
    @SuppressFBWarnings("NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE")
    private void updateCommandEntity(
        final CommandEntity entity,
        final ExecutionEnvironment resources,
        final CommandMetadata metadata,
        final List<String> executable,
        @Nullable final ComputeResources computeResources,
        final List<Criterion> clusterCriteria,
        @Nullable final Map<String, Image> images
    ) {
        this.setEntityResources(resources, entity::setConfigs, entity::setDependencies);
        this.setEntityTags(metadata.getTags(), entity::setTags);
        this.setBaseEntityMetadata(entity, metadata, resources.getSetupFile().orElse(null));

        entity.setStatus(metadata.getStatus().name());
        entity.setExecutable(executable);
        this.updateComputeResources(
            computeResources,
            entity::setCpu,
            entity::setGpu,
            entity::setMemory,
            entity::setDiskMb,
            entity::setNetworkMbps
        );
        this.updateImages(images, entity::setImages);

        this.updateClusterCriteria(entity, clusterCriteria);
    }

    private void setBaseEntityMetadata(
        final BaseEntity entity,
        final CommonMetadata metadata,
        @Nullable final String setupFile
    ) {
        // NOTE: These are all called in case someone has changed it to set something to null. DO NOT use ifPresent
        entity.setName(metadata.getName());
        entity.setUser(metadata.getUser());
        entity.setVersion(metadata.getVersion());
        entity.setDescription(metadata.getDescription().orElse(null));
        entity.setMetadata(metadata.getMetadata().orElse(null));
        entity.setSetupFile(setupFile == null ? null : this.createOrGetFileEntity(setupFile));
    }

    private void deleteApplicationEntity(final ApplicationEntity entity) throws PreconditionFailedException {
        final Set<CommandEntity> commandEntities = entity.getCommands();
        if (!commandEntities.isEmpty()) {
            throw new PreconditionFailedException(
                "Unable to delete application with id "
                    + entity.getUniqueId()
                    + " as it is still used by the following commands: "
                    + commandEntities.stream().map(CommandEntity::getUniqueId).collect(Collectors.joining())
            );
        }
        this.applicationRepository.delete(entity);
    }

    private void deleteClusterEntity(final ClusterEntity entity) {
        this.clusterRepository.delete(entity);
    }

    private void deleteCommandEntity(final CommandEntity entity) {
        //Remove the command from the associated Application references
        final List<ApplicationEntity> originalApps = entity.getApplications();
        if (originalApps != null) {
            final List<ApplicationEntity> applicationEntities = Lists.newArrayList(originalApps);
            applicationEntities.forEach(entity::removeApplication);
        }
        this.commandRepository.delete(entity);
    }

    private void deleteAllClusterCriteria(final CommandEntity commandEntity) {
        final List<CriterionEntity> persistedEntities = commandEntity.getClusterCriteria();
        final List<CriterionEntity> entitiesToDelete = Lists.newArrayList(persistedEntities);
        persistedEntities.clear();
        // Ensure Criterion aren't left dangling
        this.criterionRepository.deleteAll(entitiesToDelete);
    }

    private CriterionEntity toCriterionEntity(final Criterion criterion) {
        final CriterionEntity criterionEntity = new CriterionEntity();
        criterion.getId().ifPresent(criterionEntity::setUniqueId);
        criterion.getName().ifPresent(criterionEntity::setName);
        criterion.getVersion().ifPresent(criterionEntity::setVersion);
        criterion.getStatus().ifPresent(criterionEntity::setStatus);
        criterionEntity.setTags(this.createOrGetTagEntities(criterion.getTags()));
        return criterionEntity;
    }

    private void updateClusterCriteria(final CommandEntity commandEntity, final List<Criterion> clusterCriteria) {
        // First remove all the old criteria
        this.deleteAllClusterCriteria(commandEntity);
        // Set the new criteria
        commandEntity.setClusterCriteria(
            clusterCriteria
                .stream()
                .map(this::toCriterionEntity)
                .collect(Collectors.toList())
        );
    }

    private void setJobMetadataFields(
        final JobEntity jobEntity,
        final JobMetadata jobMetadata,
        @Nullable final String setupFile
    ) {
        this.setBaseEntityMetadata(jobEntity, jobMetadata, setupFile);
        this.setEntityTags(jobMetadata.getTags(), jobEntity::setTags);
        jobMetadata.getEmail().ifPresent(jobEntity::setEmail);
        jobMetadata.getGroup().ifPresent(jobEntity::setGenieUserGroup);
        jobMetadata.getGrouping().ifPresent(jobEntity::setGrouping);
        jobMetadata.getGroupingInstance().ifPresent(jobEntity::setGroupingInstance);
    }

    private void setJobExecutionEnvironmentFields(
        final JobEntity jobEntity,
        final ExecutionEnvironment executionEnvironment,
        @Nullable final Set<URI> savedAttachments
    ) {
        jobEntity.setConfigs(this.createOrGetFileEntities(executionEnvironment.getConfigs()));
        final Set<FileEntity> dependencies = this.createOrGetFileEntities(executionEnvironment.getDependencies());
        if (savedAttachments != null) {
            dependencies.addAll(
                this.createOrGetFileEntities(savedAttachments.stream().map(URI::toString).collect(Collectors.toSet()))
            );
        }
        jobEntity.setDependencies(dependencies);
    }

    private void setExecutionResourceCriteriaFields(
        final JobEntity jobEntity,
        final ExecutionResourceCriteria criteria
    ) {
        final List<Criterion> clusterCriteria = criteria.getClusterCriteria();
        final List<CriterionEntity> clusterCriteriaEntities
            = Lists.newArrayListWithExpectedSize(clusterCriteria.size());

        for (final Criterion clusterCriterion : clusterCriteria) {
            clusterCriteriaEntities.add(this.toCriterionEntity(clusterCriterion));
        }
        jobEntity.setClusterCriteria(clusterCriteriaEntities);
        jobEntity.setCommandCriterion(this.toCriterionEntity(criteria.getCommandCriterion()));
        jobEntity.setRequestedApplications(criteria.getApplicationIds());
    }

    private void setRequestedJobEnvironmentFields(
        final JobEntity jobEntity,
        final JobEnvironmentRequest requestedJobEnvironment
    ) {
        jobEntity.setRequestedEnvironmentVariables(requestedJobEnvironment.getRequestedEnvironmentVariables());
        this.updateComputeResources(
            requestedJobEnvironment.getRequestedComputeResources(),
            jobEntity::setRequestedCpu,
            jobEntity::setRequestedGpu,
            jobEntity::setRequestedMemory,
            jobEntity::setRequestedDiskMb,
            jobEntity::setRequestedNetworkMbps
        );
        requestedJobEnvironment.getExt().ifPresent(jobEntity::setRequestedAgentEnvironmentExt);
        this.updateImages(requestedJobEnvironment.getRequestedImages(), jobEntity::setRequestedImages);
    }

    private void setRequestedAgentConfigFields(
        final JobEntity jobEntity,
        final AgentConfigRequest requestedAgentConfig
    ) {
        jobEntity.setInteractive(requestedAgentConfig.isInteractive());
        jobEntity.setArchivingDisabled(requestedAgentConfig.isArchivingDisabled());
        requestedAgentConfig
            .getRequestedJobDirectoryLocation()
            .ifPresent(location -> jobEntity.setRequestedJobDirectoryLocation(location.getAbsolutePath()));
        requestedAgentConfig.getTimeoutRequested().ifPresent(jobEntity::setRequestedTimeout);
        requestedAgentConfig.getExt().ifPresent(jobEntity::setRequestedAgentConfigExt);
    }

    private void setRequestMetadataFields(
        final JobEntity jobEntity,
        final JobRequestMetadata jobRequestMetadata
    ) {
        jobEntity.setApi(jobRequestMetadata.isApi());
        jobEntity.setNumAttachments(jobRequestMetadata.getNumAttachments());
        jobEntity.setTotalSizeOfAttachments(jobRequestMetadata.getTotalSizeOfAttachments());
        jobRequestMetadata.getApiClientMetadata().ifPresent(
            apiClientMetadata -> {
                apiClientMetadata.getHostname().ifPresent(jobEntity::setRequestApiClientHostname);
                apiClientMetadata.getUserAgent().ifPresent(jobEntity::setRequestApiClientUserAgent);
            }
        );
        jobRequestMetadata.getAgentClientMetadata().ifPresent(
            agentClientMetadata -> {
                agentClientMetadata.getHostname().ifPresent(jobEntity::setRequestAgentClientHostname);
                agentClientMetadata.getVersion().ifPresent(jobEntity::setRequestAgentClientVersion);
                agentClientMetadata.getPid().ifPresent(jobEntity::setRequestAgentClientPid);
            }
        );
    }

    private void setExecutionResources(
        final JobEntity job,
        final String clusterId,
        final String commandId,
        final List<String> applicationIds
    ) throws NotFoundException {
        final ClusterEntity cluster = this.getClusterEntity(clusterId);
        final CommandEntity command = this.getCommandEntity(commandId);
        final List<ApplicationEntity> applications = Lists.newArrayList();
        for (final String applicationId : applicationIds) {
            applications.add(this.getApplicationEntity(applicationId));
        }

        job.setCluster(cluster);
        job.setCommand(command);
        job.setApplications(applications);
    }

    private <E extends BaseEntity> Optional<E> getEntityOrNullForFindJobs(
        final JpaBaseRepository<E> repository,
        final String id,
        @Nullable final String name
    ) {
        // User is requesting jobs using a given entity. If it doesn't exist short circuit the search
        final Optional<E> optionalEntity = repository.findByUniqueId(id);
        if (optionalEntity.isPresent()) {
            final E entity = optionalEntity.get();
            // If the name doesn't match user input request we can also short circuit search
            if (name != null && !entity.getName().equals(name)) {
                // Won't find anything matching the query
                return Optional.empty();
            }
        }

        return optionalEntity;
    }

    private SpanCustomizer addJobIdTag(final String jobId) {
        final SpanCustomizer spanCustomizer = this.tracer.currentSpanCustomizer();
        this.tagAdapter.tag(spanCustomizer, TracingConstants.JOB_ID_TAG, jobId);
        return spanCustomizer;
    }

    private void updateComputeResources(
        @Nullable final ComputeResources computeResources,
        final Consumer<Integer> cpuSetter,
        final Consumer<Integer> gpuSetter,
        final Consumer<Long> memorySetter,
        final Consumer<Long> diskMbSetter,
        final Consumer<Long> networkMbpsSetter
    ) {
        // If nothing was passed in assume it means the user desired everything to be null or missing
        if (computeResources == null) {
            cpuSetter.accept(null);
            gpuSetter.accept(null);
            memorySetter.accept(null);
            diskMbSetter.accept(null);
            networkMbpsSetter.accept(null);
        } else {
            // NOTE: These are all called in case someone has changed it to set something to null. DO NOT use ifPresent
            cpuSetter.accept(computeResources.getCpu().orElse(null));
            gpuSetter.accept(computeResources.getGpu().orElse(null));
            memorySetter.accept(computeResources.getMemoryMb().orElse(null));
            diskMbSetter.accept(computeResources.getDiskMb().orElse(null));
            networkMbpsSetter.accept(computeResources.getNetworkMbps().orElse(null));
        }
    }

    private void updateImages(
        @Nullable final Map<String, Image> images,
        final Consumer<JsonNode> imagesSetter
    ) {
        if (images == null) {
            imagesSetter.accept(null);
        } else {
            imagesSetter.accept(GenieObjectMapper.getMapper().valueToTree(images));
        }
    }
    //endregion
}
