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
package com.netflix.genie.core.jpa.services;

import com.google.common.collect.Lists;
import com.netflix.genie.common.dto.Application;
import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.dto.Job;
import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.dto.search.JobSearchResult;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.core.jpa.entities.ApplicationEntity;
import com.netflix.genie.core.jpa.entities.ClusterEntity;
import com.netflix.genie.core.jpa.entities.CommandEntity;
import com.netflix.genie.core.jpa.entities.JobEntity;
import com.netflix.genie.core.jpa.entities.JobEntity_;
import com.netflix.genie.core.jpa.entities.JobExecutionEntity;
import com.netflix.genie.core.jpa.entities.JobRequestEntity;
import com.netflix.genie.core.jpa.repositories.JpaClusterRepository;
import com.netflix.genie.core.jpa.repositories.JpaCommandRepository;
import com.netflix.genie.core.jpa.repositories.JpaJobExecutionRepository;
import com.netflix.genie.core.jpa.repositories.JpaJobRepository;
import com.netflix.genie.core.jpa.repositories.JpaJobRequestRepository;
import com.netflix.genie.core.jpa.specifications.JpaJobSpecs;
import com.netflix.genie.core.services.JobSearchService;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.validator.constraints.NotBlank;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.NoResultException;
import javax.persistence.PersistenceContext;
import javax.persistence.TypedQuery;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Order;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;
import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Jpa implementation of the Job Search Service.
 *
 * @author amsharma
 */
@Slf4j
@Transactional(readOnly = true)
public class JpaJobSearchServiceImpl implements JobSearchService {

    private final JpaJobRepository jobRepository;
    private final JpaJobRequestRepository jobRequestRepository;
    private final JpaJobExecutionRepository jobExecutionRepository;
    private final JpaClusterRepository clusterRepository;
    private final JpaCommandRepository commandRepository;

    @PersistenceContext
    private EntityManager entityManager;

    /**
     * Constructor.
     *
     * @param jobRepository          The repository to use for job entities
     * @param jobRequestRepository   The repository to use for job request entities
     * @param jobExecutionRepository The repository to use for job execution entities
     * @param clusterRepository      The repository to use for cluster entities
     * @param commandRepository      The repository to use for command entities
     */
    public JpaJobSearchServiceImpl(
        final JpaJobRepository jobRepository,
        final JpaJobRequestRepository jobRequestRepository,
        final JpaJobExecutionRepository jobExecutionRepository,
        final JpaClusterRepository clusterRepository,
        final JpaCommandRepository commandRepository
    ) {
        this.jobRepository = jobRepository;
        this.jobRequestRepository = jobRequestRepository;
        this.jobExecutionRepository = jobExecutionRepository;
        this.clusterRepository = clusterRepository;
        this.commandRepository = commandRepository;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Page<JobSearchResult> findJobs(
        final String id,
        final String jobName,
        final String user,
        final Set<JobStatus> statuses,
        final Set<String> tags,
        final String clusterName,
        final String clusterId,
        final String commandName,
        final String commandId,
        final Date minStarted,
        final Date maxStarted,
        final Date minFinished,
        final Date maxFinished,
        @NotNull final Pageable page
    ) {
        log.debug("called");

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
                clusterId == null ? null : this.clusterRepository.findOne(clusterId),
                commandName,
                commandId == null ? null : this.commandRepository.findOne(commandId),
                minStarted,
                maxStarted,
                minFinished,
                maxFinished
            );

        countQuery.select(cb.count(root)).where(whereClause);

        final Long count = this.entityManager.createQuery(countQuery).getSingleResult();

        // Use the count to make sure we even need to make this query
        if (count > 0) {
            final CriteriaQuery<JobSearchResult> contentQuery = cb.createQuery(JobSearchResult.class);
            contentQuery.from(JobEntity.class);

            contentQuery.multiselect(
                root.get(JobEntity_.id),
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
                .setFirstResult(page.getOffset())
                .setMaxResults(page.getPageSize())
                .getResultList();

            return new PageImpl<>(results, page, count);
        } else {
            return new PageImpl<>(Lists.newArrayList());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<Job> getAllActiveJobsOnHost(@NotBlank final String hostName) {
        log.debug("Called with hostname {}", hostName);

        final TypedQuery<JobEntity> query = entityManager
            .createNamedQuery(JobExecutionEntity.QUERY_FIND_BY_STATUS_HOST, JobEntity.class);
        query.setParameter("statuses", JobStatus.getActiveStatuses());
        query.setParameter("hostName", hostName);

        return query
            .getResultList()
            .stream()
            .map(JobEntity::getDTO)
            .collect(Collectors.toSet());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> getAllHostsWithActiveJobs() {
        log.debug("Called");

        final TypedQuery<String> query = entityManager
            .createNamedQuery(JobExecutionEntity.QUERY_FIND_HOSTS_BY_STATUS, String.class);
        query.setParameter("statuses", JobStatus.getActiveStatuses());

        return query.getResultList();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Job getJob(
        @NotBlank(message = "No id entered. Unable to get job.") final String id
    ) throws GenieNotFoundException {
        log.debug("Called with id {}", id);
        final JobEntity jobEntity = this.jobRepository.findOne(id);
        if (jobEntity != null) {
            return jobEntity.getDTO();
        } else {
            throw new GenieNotFoundException("No job with id " + id);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JobStatus getJobStatus(@NotBlank final String id) throws GenieException {
        log.debug("Called with id {}", id);
        final TypedQuery<JobStatus> query = entityManager
            .createNamedQuery(JobEntity.QUERY_GET_STATUS_BY_ID, JobStatus.class);
        query.setParameter("id", id);
        try {
            return query.getSingleResult();
        } catch (NoResultException e) {
            throw new GenieNotFoundException("No job with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JobRequest getJobRequest(@NotBlank final String id) throws GenieException {
        log.debug("Called with id {}", id);
        final JobRequestEntity jobRequestEntity = this.jobRequestRepository.findOne(id);
        if (jobRequestEntity != null) {
            return jobRequestEntity.getDTO();
        } else {
            throw new GenieNotFoundException("No job request with id " + id);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JobExecution getJobExecution(@NotBlank final String id) throws GenieException {
        log.debug("Called with id {}", id);
        final JobExecutionEntity jobExecutionEntity = this.jobExecutionRepository.findOne(id);
        if (jobExecutionEntity != null) {
            return jobExecutionEntity.getDTO();
        } else {
            throw new GenieNotFoundException("No job execution with id " + id);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Cluster getJobCluster(@NotBlank final String id) throws GenieException {
        log.debug("Called for job with id {}", id);
        final JobEntity job = this.jobRepository.findOne(id);
        if (job != null) {
            final ClusterEntity cluster = job.getCluster();
            if (cluster != null) {
                return cluster.getDTO();
            } else {
                throw new GenieNotFoundException("Job " + id + " doesn't have a cluster associated with it");
            }
        } else {
            throw new GenieNotFoundException("No job with id " + id + " exists. Unable to get cluster");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Command getJobCommand(@NotBlank final String id) throws GenieException {
        log.debug("Called for job with id {}", id);
        final JobEntity job = this.jobRepository.findOne(id);
        if (job != null) {
            final CommandEntity command = job.getCommand();
            if (command != null) {
                return command.getDTO();
            } else {
                throw new GenieNotFoundException("Job " + id + " doesn't have a command associated with it");
            }
        } else {
            throw new GenieNotFoundException("No job with id " + id + " exists. Unable to get command");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Application> getJobApplications(@NotBlank final String id) throws GenieException {
        log.debug("Called for job with id {}", id);
        final JobEntity job = this.jobRepository.findOne(id);
        if (job != null) {
            final List<ApplicationEntity> applications = job.getApplications();
            if (applications != null && !applications.isEmpty()) {
                return applications.stream().map(ApplicationEntity::getDTO).collect(Collectors.toList());
            } else {
                return Collections.EMPTY_LIST;
            }
        } else {
            throw new GenieNotFoundException("No job with id " + id + " exists. Unable to get cluster");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getJobHost(@NotBlank final String jobId) throws GenieException {
        final JobExecutionEntity jobExecution = this.jobExecutionRepository.findOne(jobId);
        if (jobExecution != null) {
            return jobExecution.getHostName();
        } else {
            throw new GenieNotFoundException("No job execution found for id " + jobId);
        }
    }
}
