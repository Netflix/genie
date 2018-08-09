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
package com.netflix.genie.web.jpa.services;

import com.google.common.collect.Lists;
import com.netflix.genie.common.dto.Application;
import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.dto.Job;
import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.common.dto.JobMetadata;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.dto.search.JobSearchResult;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.web.jpa.entities.JobEntity;
import com.netflix.genie.web.jpa.entities.JobEntity_;
import com.netflix.genie.web.jpa.entities.projections.AgentHostnameProjection;
import com.netflix.genie.web.jpa.entities.projections.JobApplicationsProjection;
import com.netflix.genie.web.jpa.entities.projections.JobClusterProjection;
import com.netflix.genie.web.jpa.entities.projections.JobCommandProjection;
import com.netflix.genie.web.jpa.entities.projections.JobExecutionProjection;
import com.netflix.genie.web.jpa.entities.projections.JobMetadataProjection;
import com.netflix.genie.web.jpa.entities.projections.JobProjection;
import com.netflix.genie.web.jpa.entities.projections.JobRequestProjection;
import com.netflix.genie.web.jpa.entities.projections.JobStatusProjection;
import com.netflix.genie.web.jpa.repositories.JpaClusterRepository;
import com.netflix.genie.web.jpa.repositories.JpaCommandRepository;
import com.netflix.genie.web.jpa.repositories.JpaJobRepository;
import com.netflix.genie.web.jpa.specifications.JpaJobSpecs;
import com.netflix.genie.web.services.JobSearchService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Nullable;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Order;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * JPA implementation of the Job Search Service.
 *
 * @author amsharma
 * @author tgianos
 */
@Transactional(readOnly = true)
@Slf4j
public class JpaJobSearchServiceImpl implements JobSearchService {

    private final JpaJobRepository jobRepository;
    private final JpaClusterRepository clusterRepository;
    private final JpaCommandRepository commandRepository;

    @PersistenceContext
    private EntityManager entityManager;

    /**
     * Constructor.
     *
     * @param jobRepository     The repository to use for job entities
     * @param clusterRepository The repository to use for cluster entities
     * @param commandRepository The repository to use for command entities
     */
    public JpaJobSearchServiceImpl(
        final JpaJobRepository jobRepository,
        final JpaClusterRepository clusterRepository,
        final JpaCommandRepository commandRepository
    ) {
        this.jobRepository = jobRepository;
        this.clusterRepository = clusterRepository;
        this.commandRepository = commandRepository;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("checkstyle:parameternumber")
    public Page<JobSearchResult> findJobs(
        @Nullable final String id,
        @Nullable final String jobName,
        @Nullable final String user,
        @Nullable final Set<JobStatus> statuses,
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
        log.debug("called");

        // TODO: Re-write with projections however not currently supported: https://jira.spring.io/browse/DATAJPA-1033
        final CriteriaBuilder cb = this.entityManager.getCriteriaBuilder();
        final CriteriaQuery<Long> countQuery = cb.createQuery(Long.class);
        final Root<JobEntity> root = countQuery.from(JobEntity.class);

        final Predicate whereClause = JpaJobSpecs
            .getFindPredicate(
                root,
                cb,
                id,
                jobName,
                user,
                statuses,
                tags,
                clusterName,
                clusterId == null ? null : this.clusterRepository.findByUniqueId(clusterId).orElse(null),
                commandName,
                commandId == null ? null : this.commandRepository.findByUniqueId(commandId).orElse(null),
                minStarted,
                maxStarted,
                minFinished,
                maxFinished,
                grouping,
                groupingInstance
            );

        countQuery.select(cb.count(root)).where(whereClause);

        final Long count = this.entityManager.createQuery(countQuery).getSingleResult();

        // Use the count to make sure we even need to make this query
        if (count > 0) {
            final CriteriaQuery<JobSearchResult> contentQuery = cb.createQuery(JobSearchResult.class);
            contentQuery.from(JobEntity.class);

            contentQuery.multiselect(
                root.get(JobEntity_.uniqueId),
                root.get(JobEntity_.name),
                root.get(JobEntity_.user),
                root.get(JobEntity_.status),
                root.get(JobEntity_.started),
                root.get(JobEntity_.finished),
                root.get(JobEntity_.clusterName),
                root.get(JobEntity_.commandName)
            );

            contentQuery.where(whereClause);

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

            return new PageImpl<>(results, page, count);
        } else {
            return new PageImpl<>(Lists.newArrayList(), page, count);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<Job> getAllActiveJobsOnHost(@NotBlank final String hostname) {
        log.debug("Called with hostname {}", hostname);

        final Set<JobProjection> jobs
            = this.jobRepository.findByAgentHostnameAndStatusIn(hostname, JobStatus.getActiveStatuses());

        return jobs
            .stream()
            .map(JpaServiceUtils::toJobDto)
            .collect(Collectors.toSet());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<String> getAllHostsWithActiveJobs() {
        log.debug("Called");

        return this.jobRepository
            .findDistinctByStatusInAndV4IsFalse(JobStatus.getActiveStatuses())
            .stream()
            .map(AgentHostnameProjection::getAgentHostname)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toSet());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Job getJob(
        @NotBlank(message = "No id entered. Unable to get job.") final String id
    ) throws GenieNotFoundException {
        log.debug("Called with id {}", id);
        return JpaServiceUtils.toJobDto(
            this.jobRepository
                .findByUniqueId(id, JobProjection.class)
                .orElseThrow(() -> new GenieNotFoundException("No job with id " + id))
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JobStatus getJobStatus(@NotBlank final String id) throws GenieException {
        log.debug("Called with id {}", id);
        return this.jobRepository
            .findByUniqueId(id, JobStatusProjection.class)
            .orElseThrow(() -> new GenieNotFoundException("No job with id " + id + " exists."))
            .getStatus();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JobRequest getJobRequest(@NotBlank final String id) throws GenieException {
        log.debug("Called with id {}", id);
        return JpaServiceUtils.toJobRequestDto(
            this.jobRepository
                .findByUniqueId(id, JobRequestProjection.class)
                .orElseThrow(() -> new GenieNotFoundException("No job request with id " + id))
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JobExecution getJobExecution(@NotBlank final String id) throws GenieException {
        log.debug("Called with id {}", id);
        return JpaServiceUtils.toJobExecutionDto(
            this.jobRepository
                .findByUniqueId(id, JobExecutionProjection.class)
                .orElseThrow(() -> new GenieNotFoundException("No job execution with id " + id))
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Cluster getJobCluster(@NotBlank final String id) throws GenieException {
        log.debug("Called for job with id {}", id);
        return JpaServiceUtils.toClusterDto(
            this.jobRepository
                .findByUniqueId(id, JobClusterProjection.class)
                .orElseThrow(
                    () -> new GenieNotFoundException("No job with id " + id + " exists. Unable to get cluster")
                )
                .getCluster()
                .orElseThrow(
                    () -> new GenieNotFoundException("Job " + id + " doesn't have a cluster associated with it")
                )
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Command getJobCommand(@NotBlank final String id) throws GenieException {
        log.debug("Called for job with id {}", id);
        return JpaServiceUtils.toCommandDto(
            this.jobRepository
                .findByUniqueId(id, JobCommandProjection.class)
                .orElseThrow(
                    () -> new GenieNotFoundException("No job with id " + id + " exists. Unable to get command")
                )
                .getCommand()
                .orElseThrow(
                    () -> new GenieNotFoundException("Job " + id + " doesn't have a command associated with it")
                )
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Application> getJobApplications(@NotBlank final String id) throws GenieException {
        log.debug("Called for job with id {}", id);
        return this.jobRepository.findByUniqueId(id, JobApplicationsProjection.class)
            .orElseThrow(() -> new GenieNotFoundException("No job with " + id + " exists. Unable to get applications"))
            .getApplications()
            .stream()
            .map(JpaServiceUtils::toApplicationDto)
            .collect(Collectors.toList());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getJobHost(@NotBlank final String jobId) throws GenieNotFoundException {
        return this.jobRepository
            .findByUniqueId(jobId, AgentHostnameProjection.class)
            .orElseThrow(() -> new GenieNotFoundException("No job execution found for id " + jobId))
            .getAgentHostname()
            .orElseThrow(() -> new GenieNotFoundException("No hostname set for job " + jobId));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getActiveJobCountForUser(@NotBlank final String user) throws GenieException {
        log.debug("Called for jobs with user {}", user);
        final Long count = this.jobRepository.countJobsByUserAndStatusIn(user, JobStatus.getActiveStatuses());
        if (count == null || count < 0) {
            throw new GenieServerException(
                "Count query for user "
                    + user
                    + "produced an unexpected result: "
                    + count
            );
        }
        return count;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JobMetadata getJobMetadata(@NotBlank final String id) throws GenieException {
        return JpaServiceUtils.toJobMetadataDto(
            this.jobRepository
                .findByUniqueId(id, JobMetadataProjection.class)
                .orElseThrow(() -> new GenieNotFoundException("No job metadata found for id " + id))
        );
    }
}
