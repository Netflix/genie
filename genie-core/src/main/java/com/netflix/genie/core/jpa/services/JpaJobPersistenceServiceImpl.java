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

import com.netflix.genie.common.dto.Job;

import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.core.jpa.entities.JobEntity;
import com.netflix.genie.core.jpa.repositories.JpaJobRepository;
import com.netflix.genie.core.services.JobPersistenceService;
import org.hibernate.validator.constraints.NotBlank;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.validation.constraints.NotNull;

/**
 * JPA implementation of the job persistence service.
 *
 * @author amsharma
 */
@Service
public class JpaJobPersistenceServiceImpl implements JobPersistenceService {

    private static final Logger LOG = LoggerFactory.getLogger(JpaJobPersistenceServiceImpl.class);
    private final JpaJobRepository jobRepo;

    /**
     * Default Constructor.
     *
     * @param jobRepo The job repository to use
     */
    @Autowired
    public JpaJobPersistenceServiceImpl(
            final JpaJobRepository jobRepo
    ) {
        this.jobRepo = jobRepo;
    }
    /**
     {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public Job getJob(
            @NotBlank(message = "No id entered. Unable to get job.")
            final String id
    ) throws GenieException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Called with id " + id);
        }
        return this.findJob(id).getDTO();
    }

    /**
     {@inheritDoc}
     */
    @Override
    public void saveJob(
            @NotNull(message = "Job is null so cannot be saved")
            final Job job
    ) throws GenieException {

    }

    /**
     * Helper to find an application entity based on ID.
     *
     * @param id The id of the application to find
     * @return The application entity if one is found
     * @throws GenieNotFoundException If no application is found
     */
    private JobEntity findJob(final String id)
            throws GenieNotFoundException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Called");
        }
        final JobEntity jobEntity = this.jobRepo.findOne(id);
        if (jobEntity != null) {
            return jobEntity;
        } else {
            throw new GenieNotFoundException("No job with id " + id);
        }
    }
}
