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
import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.ApplicationStatus;
import com.netflix.genie.common.exceptions.GenieBadRequestException;
import com.netflix.genie.common.exceptions.GenieConflictException;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.internal.dto.v4.Application;
import com.netflix.genie.common.internal.dto.v4.ApplicationMetadata;
import com.netflix.genie.common.internal.dto.v4.ApplicationRequest;
import com.netflix.genie.common.internal.dto.v4.ExecutionEnvironment;
import com.netflix.genie.test.categories.UnitTest;
import com.netflix.genie.web.jpa.entities.ApplicationEntity;
import com.netflix.genie.web.jpa.entities.CommandEntity;
import com.netflix.genie.web.jpa.repositories.JpaApplicationRepository;
import com.netflix.genie.web.jpa.repositories.JpaClusterRepository;
import com.netflix.genie.web.jpa.repositories.JpaCommandRepository;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.springframework.dao.DuplicateKeyException;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

/**
 * Tests for the ApplicationServiceJPAImpl.
 *
 * @author tgianos
 */
@Category(UnitTest.class)
public class JpaApplicationPersistenceServiceImplUnitTests {

    private static final String APP_1_ID = "app1";
    private static final String APP_1_NAME = "tez";
    private static final String APP_1_USER = "tgianos";
    private static final String APP_1_VERSION = "1.2.3";

    private JpaApplicationRepository jpaApplicationRepository;
    private JpaApplicationPersistenceServiceImpl appService;

    /**
     * Setup the tests.
     */
    @Before
    public void setup() {
        this.jpaApplicationRepository = Mockito.mock(JpaApplicationRepository.class);
        this.appService = new JpaApplicationPersistenceServiceImpl(
            Mockito.mock(JpaTagPersistenceService.class),
            Mockito.mock(JpaFilePersistenceService.class),
            this.jpaApplicationRepository,
            Mockito.mock(JpaClusterRepository.class),
            Mockito.mock(JpaCommandRepository.class)
        );
    }

    /**
     * Test the get application method.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testGetApplicationNotExists() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaApplicationRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        this.appService.getApplication(id);
    }

    /**
     * Test to make sure an exception is thrown when application already exists.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieConflictException.class)
    public void testCreateApplicationAlreadyExists() throws GenieException {
        final ApplicationRequest request = new ApplicationRequest.Builder(
            new ApplicationMetadata.Builder(
                APP_1_NAME,
                APP_1_USER,
                APP_1_VERSION,
                ApplicationStatus.ACTIVE
            )
                .build()
        )
            .withRequestedId(APP_1_ID)
            .build();

        Mockito
            .when(this.jpaApplicationRepository.save(Mockito.any(ApplicationEntity.class)))
            .thenThrow(new DuplicateKeyException("Duplicate Key"));
        this.appService.createApplication(request);
    }

    /**
     * Test to update an application.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testUpdateApplicationNoAppExists() throws GenieException {
        final Application app = new Application(
            APP_1_ID,
            Instant.now(),
            Instant.now(),
            new ExecutionEnvironment(null, null, null),
            new ApplicationMetadata.Builder(
                APP_1_NAME,
                APP_1_USER,
                APP_1_VERSION,
                ApplicationStatus.ACTIVE
            )
                .build()
        );
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaApplicationRepository.findByUniqueId(Mockito.eq(id))).thenReturn(Optional.empty());
        this.appService.updateApplication(id, app);
    }

    /**
     * Test to update an application.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieBadRequestException.class)
    public void testUpdateApplicationIdsDontMatch() throws GenieException {
        final String id = UUID.randomUUID().toString();
        final Application app = new Application(
            UUID.randomUUID().toString(),
            Instant.now(),
            Instant.now(),
            new ExecutionEnvironment(null, null, null),
            new ApplicationMetadata.Builder(
                APP_1_NAME,
                APP_1_USER,
                APP_1_VERSION,
                ApplicationStatus.ACTIVE
            )
                .build()
        );
        Mockito.when(this.jpaApplicationRepository.existsByUniqueId(id)).thenReturn(true);
        this.appService.updateApplication(id, app);
    }

    /**
     * Test delete all when still in a relationship with a command.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GeniePreconditionException.class)
    public void testDeleteAllBlocked() throws GenieException {
        final ApplicationEntity applicationEntity
            = Mockito.mock(ApplicationEntity.class);
        final CommandEntity commandEntity = Mockito.mock(CommandEntity.class);
        Mockito.when(this.jpaApplicationRepository.findAll()).thenReturn(Lists.newArrayList(applicationEntity));
        Mockito.when(applicationEntity.getCommands()).thenReturn(Sets.newHashSet(commandEntity));
        this.appService.deleteAllApplications();
    }

    /**
     * Test delete all still runs if commands is null.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testDeleteAllNullCommands() throws GenieException {
        final ApplicationEntity applicationEntity = Mockito.mock(ApplicationEntity.class);
        Mockito.when(this.jpaApplicationRepository.findAll()).thenReturn(Lists.newArrayList(applicationEntity));
        Mockito.when(applicationEntity.getCommands()).thenReturn(null);
        this.appService.deleteAllApplications();
        Mockito.verify(this.jpaApplicationRepository, Mockito.times(1)).deleteAll();
    }

    /**
     * Test delete.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieException.class)
    public void testDeleteNoAppToDelete() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaApplicationRepository.findByUniqueId(Mockito.eq(id))).thenReturn(Optional.empty());
        this.appService.deleteApplication(id);
    }

    /**
     * Test add configurations to application.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testAddConfigsToApplicationNoApp() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaApplicationRepository.findByUniqueId(Mockito.eq(id))).thenReturn(Optional.empty());
        this.appService.addConfigsToApplication(id, Sets.newHashSet());
    }

    /**
     * Test update configurations for application.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testUpdateConfigsForApplicationNoApp() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaApplicationRepository.findByUniqueId(Mockito.eq(id))).thenReturn(Optional.empty());
        this.appService.updateConfigsForApplication(id, Sets.newHashSet());
    }

    /**
     * Test get configurations to application.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testGetConfigsForApplicationNoApp() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaApplicationRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        this.appService.getConfigsForApplication(id);
    }

    /**
     * Test remove all configurations for application.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testRemoveAllConfigsForApplicationNoApp() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaApplicationRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        this.appService.removeAllConfigsForApplication(id);
    }

    /**
     * Test remove configuration for application.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testRemoveConfigForApplicationNoApp() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaApplicationRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        this.appService.removeConfigForApplication(id, "something");
    }

    /**
     * Test add jars to application.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testAddJarsForApplicationNoApp() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaApplicationRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        this.appService.addDependenciesForApplication(id, Sets.newHashSet());
    }

    /**
     * Test update jars for application.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testUpdateJarsForApplicationNoApp() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaApplicationRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        this.appService.updateDependenciesForApplication(id, Sets.newHashSet());
    }

    /**
     * Test get jars to application.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testGetJarsForApplicationNoApp() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaApplicationRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        this.appService.getDependenciesForApplication(id);
    }

    /**
     * Test remove all jars for application.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testRemoveAllJarsForApplicationNoApp() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaApplicationRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        this.appService.removeAllDependenciesForApplication(id);
    }

    /**
     * Test remove configuration for application.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testRemoveJarForApplicationNoApp() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaApplicationRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        this.appService.removeDependencyForApplication(id, "something");
    }

    /**
     * Test add tags to application.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testAddTagsForApplicationNoApp() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaApplicationRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        this.appService.addTagsForApplication(id, Sets.newHashSet());
    }

    /**
     * Test update tags for application.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testUpdateTagsForApplicationNoApp() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaApplicationRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        this.appService.updateTagsForApplication(id, Sets.newHashSet());
    }

    /**
     * Test get tags to application.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testGetTagsForApplicationNoApp() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaApplicationRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        this.appService.getTagsForApplication(id);
    }

    /**
     * Test remove all tags for application.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testRemoveAllTagsForApplicationNoApp() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaApplicationRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        this.appService.removeAllTagsForApplication(id);
    }

    /**
     * Test remove configuration for application.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testRemoveTagForApplicationNoApp() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaApplicationRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        this.appService.removeTagForApplication(id, "something");
    }

    /**
     * Test the Get commands for application method.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testGetCommandsForApplicationNoApp() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jpaApplicationRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        this.appService.getCommandsForApplication(id, null);
    }
}
