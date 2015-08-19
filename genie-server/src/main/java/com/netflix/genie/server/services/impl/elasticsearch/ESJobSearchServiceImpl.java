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
package com.netflix.genie.server.services.impl.elasticsearch;

import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.model.Job;
import com.netflix.genie.common.model.JobStatus;
import com.netflix.genie.server.repository.elasticsearch.ESJobRepository;
import com.netflix.genie.server.services.JobSearchService;
import org.hibernate.validator.constraints.NotBlank;
import org.springframework.data.domain.PageRequest;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.List;
import java.util.Set;

/**
 * Elasticsearch based job search service implementation.
 *
 * @author tgianos
 */
@Named
public class ESJobSearchServiceImpl implements JobSearchService {

    private final ESJobRepository repository;

    /**
     * Constructor.
     *
     * @param repository The elastic search repository.
     */
    @Inject
    public ESJobSearchServiceImpl(final ESJobRepository repository) {
        this.repository = repository;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Job getJob(@NotBlank(message = "No id entered. Unable to get job.") final String id) throws GenieException {
        return this.repository.findOne(id);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Job> getJobs(
            final String id,
            final String jobName,
            final String userName,
            final Set<JobStatus> statuses,
            final Set<String> tags,
            final String clusterName,
            final String clusterId,
            final String commandName,
            final String commandId,
            final int page,
            final int limit,
            final boolean descending,
            final Set<String> orderBys
    ) {
        final PageRequest pageRequest = new PageRequest(page, limit);
        return this.repository.findByTagsContains("kragle", pageRequest);
    }
}
