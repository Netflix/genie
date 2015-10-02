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
package com.netflix.genie.core.elasticsearch.services;

import com.netflix.genie.common.dto.Job;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.core.elasticsearch.repositories.ESJobRepository;
import com.netflix.genie.core.jpa.entities.JobEntity;
import com.netflix.genie.core.services.JobSearchService;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.validator.constraints.NotBlank;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.query.Criteria;
import org.springframework.data.elasticsearch.core.query.CriteriaQuery;
import org.springframework.stereotype.Service;

import java.util.Set;

/**
 * Elasticsearch based job search service implementation.
 *
 * @author tgianos
 */
@Service
public class ESJobSearchServiceImpl implements JobSearchService {

    private final ESJobRepository repository;
    private final ElasticsearchTemplate template;

    /**
     * Constructor.
     *
     * @param repository The elastic search repository
     * @param template   The elastic search template
     */
    @Autowired
    public ESJobSearchServiceImpl(final ESJobRepository repository, final ElasticsearchTemplate template) {
        this.repository = repository;
        this.template = template;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Job getJob(@NotBlank(message = "No id entered. Unable to get job.") final String id) throws GenieException {
        final JobEntity job = this.repository.findOne(id);
        if (job != null) {
            return job.getDTO();
        } else {
            throw new GenieNotFoundException("No job with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Page<Job> getJobs(
            final String id,
            final String jobName,
            final String userName,
            final Set<JobStatus> statuses,
            final Set<String> tags,
            final String clusterName,
            final String clusterId,
            final String commandName,
            final String commandId,
            final Pageable page
    ) {
        Criteria criteria = null;
        if (StringUtils.isNotBlank(id)) {
            criteria = new Criteria("id").contains(id);
        }
        if (StringUtils.isNotBlank(jobName)) {
            if (criteria == null) {
                criteria = new Criteria("name").contains(jobName);
            } else {
                criteria.and(new Criteria("name").contains(jobName));
            }
        }
        if (StringUtils.isNotBlank(userName)) {
            if (criteria == null) {
                criteria = new Criteria("user").is(userName);
            } else {
                criteria.and(new Criteria("user").is(userName));
            }
        }
        if (statuses != null && !statuses.isEmpty()) {
            final Criteria statusCriteria = new Criteria("status");
            statuses.stream().forEach(status -> statusCriteria.or(new Criteria("status").is(status.toString())));

            if (criteria == null) {
                criteria = statusCriteria;
            } else {
                criteria.and(statusCriteria);
            }
        }
        if (tags != null && !tags.isEmpty()) {
            for (final String tag : tags) {
                if (StringUtils.isNotBlank(tag)) {
                    if (criteria == null) {
                        criteria = new Criteria("tags").contains(tag);
                    } else {
                        criteria.and(new Criteria("tags").contains(tag));
                    }
                }
            }
        }
        if (StringUtils.isNotBlank(clusterName)) {
            if (criteria == null) {
                criteria = new Criteria("executionClusterName").is(clusterName);
            } else {
                criteria.and("executionClusterName").is(clusterName);
            }
        }
        if (StringUtils.isNotBlank(clusterId)) {
            if (criteria == null) {
                criteria = new Criteria("clusterId").is(clusterId);
            } else {
                criteria.and("clusterId").is(clusterId);
            }
        }
        if (StringUtils.isNotBlank(commandName)) {
            if (criteria == null) {
                criteria = new Criteria("commandName").is(commandName);
            } else {
                criteria.and("commandName").is(commandName);
            }
        }
        if (StringUtils.isNotBlank(commandId)) {
            if (criteria == null) {
                criteria = new Criteria("commandId").is(commandId);
            } else {
                criteria.and("commandId").is(commandId);
            }
        }

        if (criteria != null) {
            final CriteriaQuery query = new CriteriaQuery(criteria, page);
            return this.template
                    .queryForPage(query, JobEntity.class)
                    .map(JobEntity::getDTO);
        } else {
            return this.repository.findAll(page).map(JobEntity::getDTO);
        }
    }
}
