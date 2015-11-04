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
import com.netflix.genie.core.elasticsearch.documents.JobDocument;
import com.netflix.genie.core.elasticsearch.repositories.EsJobRepository;
import com.netflix.genie.core.services.JobPersistenceService;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.hibernate.validator.constraints.NotBlank;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.data.elasticsearch.core.query.SearchQuery;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Elasticsearch based job search service implementation.
 *
 * @author tgianos
 */
@Service
public class EsJobPersistenceServiceImpl implements JobPersistenceService {

    private static final Logger LOG = LoggerFactory.getLogger(EsJobPersistenceServiceImpl.class);

    private final EsJobRepository repository;
    private final ElasticsearchTemplate template;

    /**
     * Constructor.
     *
     * @param repository The elastic search repository
     * @param template   The elastic search template
     */
    @Autowired
    public EsJobPersistenceServiceImpl(final EsJobRepository repository, final ElasticsearchTemplate template) {
        this.repository = repository;
        this.template = template;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Job getJob(@NotBlank(message = "No id entered. Unable to get job.") final String id) throws GenieException {
        final JobDocument job = this.repository.findOne(id);
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
        final List<FilterBuilder> filters = new ArrayList<>();
        final BoolQueryBuilder builder = QueryBuilders.boolQuery();

        if (StringUtils.isNotBlank(id)) {
            //TODO: is this the right builder to use?
            builder.must(QueryBuilders.commonTermsQuery("id", id));
        }
        if (StringUtils.isNotBlank(jobName)) {
            builder.must(QueryBuilders.commonTermsQuery("name", jobName));
        }
        if (StringUtils.isNotBlank(userName)) {
            filters.add(FilterBuilders.termFilter("user", userName));
        }
        if (statuses != null && !statuses.isEmpty()) {
            filters.add(
                    FilterBuilders.termsFilter(
                            "status",
                            statuses
                                    .stream()
                                    .map(JobStatus::toString)
                                    .map(String::toLowerCase)
                                    .collect(Collectors.toList())
                    )
            );
        }
        if (tags != null && !tags.isEmpty()) {
            filters.add(FilterBuilders.termsFilter("tags", tags));
        }
        if (StringUtils.isNotBlank(clusterName)) {
            filters.add(FilterBuilders.termFilter("executionClusterName", clusterName));
        }
        if (StringUtils.isNotBlank(clusterId)) {
            filters.add(FilterBuilders.termFilter("executionClusterId", clusterId));
        }
        if (StringUtils.isNotBlank(commandName)) {
            filters.add(FilterBuilders.termFilter("commandName", commandName));
        }
        if (StringUtils.isNotBlank(commandId)) {
            filters.add(FilterBuilders.termFilter("commandId", commandId));
        }

        final FilterBuilder finalFilter = FilterBuilders.andFilter(filters.toArray(new FilterBuilder[filters.size()]));
//        if (LOG.isDebugEnabled()) {
        LOG.info("Query is: " + builder.toString());
        LOG.info("Filter is: " + finalFilter.toString());
        LOG.info("Page is: " + page);
//        }
        final SearchQuery query = new NativeSearchQueryBuilder()
                .withFilter(finalFilter)
                .withQuery(builder)
                .withPageable(page)
                .build();
        return this.template
                .queryForPage(query, JobDocument.class)
                .map(JobDocument::getDTO);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void saveJob(
            final Job job
    ) {

    }
}
