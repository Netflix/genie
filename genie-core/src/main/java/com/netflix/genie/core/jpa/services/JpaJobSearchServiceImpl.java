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
import com.netflix.genie.common.dto.Job;
import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.dto.search.JobSearchResult;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.core.jpa.entities.JobEntity;
import com.netflix.genie.core.jpa.entities.JobEntity_;
import com.netflix.genie.core.jpa.entities.JobExecutionEntity;
import com.netflix.genie.core.jpa.repositories.JpaJobExecutionRepository;
import com.netflix.genie.core.jpa.repositories.JpaJobRepository;
import com.netflix.genie.core.jpa.specifications.JpaJobSpecs;
import com.netflix.genie.core.services.JobSearchService;
import org.hibernate.validator.constraints.NotBlank;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Order;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;
import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Jpa implementation of the Job Search Service.
 *
 * @author amsharma
 */
@Service
public class JpaJobSearchServiceImpl implements JobSearchService {

    private static final Logger LOG = LoggerFactory.getLogger(JpaJobSearchServiceImpl.class);
    private final JpaJobRepository jobRepository;
    private final JpaJobExecutionRepository jobExecutionRepository;

    @PersistenceContext
    private EntityManager entityManager;

    /**
     * Constructor.
     *
     * @param jobRepository          The repository to use for job objects
     * @param jobExecutionRepository The repository to use for job execution objects
     */
    @Autowired
    public JpaJobSearchServiceImpl(
        final JpaJobRepository jobRepository,
        final JpaJobExecutionRepository jobExecutionRepository
    ) {
        this.jobRepository = jobRepository;
        this.jobExecutionRepository = jobExecutionRepository;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public Page<JobSearchResult> findJobs(
        final String id,
        final String jobName,
        final String userName,
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
        LOG.debug("called");

        final CriteriaBuilder cb = this.entityManager.getCriteriaBuilder();
        final CriteriaQuery<Long> countQuery = cb.createQuery(Long.class);
        final Root<JobEntity> root = countQuery.from(JobEntity.class);

        final Predicate whereClause = JpaJobSpecs
            .getFindPredicate(
                root,
                cb,
                id,
                jobName,
                userName,
                statuses,
                tags,
                clusterName,
                clusterId,
                commandName,
                commandId,
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
    @Transactional(readOnly = true)
    public Set<JobExecution> getAllRunningJobExecutionsOnHost(@NotBlank final String hostname) throws GenieException {
        final Set<JobExecutionEntity> entities
            = this.jobExecutionRepository.findByHostnameAndExitCode(hostname, JobExecution.DEFAULT_EXIT_CODE);

        final Set<JobExecution> executions = new HashSet<>();
        for (final JobExecutionEntity entity : entities) {
            executions.add(entity.getDTO());
        }
        return executions;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public Job getJob(
        @NotBlank(message = "No id entered. Unable to get job.")
        final String id
    ) throws GenieNotFoundException {
        LOG.debug("Called");
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
        if (this.jobRepository.exists(id)) {
            final CriteriaBuilder cb = this.entityManager.getCriteriaBuilder();
            final CriteriaQuery<JobStatus> query = cb.createQuery(JobStatus.class);
            final Root<JobEntity> root = query.from(JobEntity.class);
            query.select(root.get(JobEntity_.status));
            query.where(cb.equal(root.get(JobEntity_.id), id));
            return this.entityManager.createQuery(query).getSingleResult();
        } else {
            throw new GenieNotFoundException("No job with id " + id + " exists.");
        }
    }

    /**
     * Set the entity manager to use for transactions in this service.
     *
     * @param entityManager The entity manager to use.
     */
    protected void setEntityManager(final EntityManager entityManager) {
        this.entityManager = entityManager;
    }
}
