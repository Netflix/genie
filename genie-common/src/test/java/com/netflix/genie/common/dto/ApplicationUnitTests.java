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
package com.netflix.genie.common.dto;

import com.google.common.collect.Sets;
import com.netflix.genie.test.categories.UnitTest;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Date;
import java.util.Set;
import java.util.UUID;

/**
 * Tests for the Application DTO.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class ApplicationUnitTests {

    private static final String NAME = UUID.randomUUID().toString();
    private static final String USER = UUID.randomUUID().toString();
    private static final String VERSION = UUID.randomUUID().toString();

    /**
     * Test to make sure we can build an application using the default builder constructor.
     */
    @Test
    public void canBuildApplication() {
        final Application application = new Application.Builder(NAME, USER, VERSION, ApplicationStatus.ACTIVE).build();
        Assert.assertThat(application.getName(), Matchers.is(NAME));
        Assert.assertThat(application.getUser(), Matchers.is(USER));
        Assert.assertThat(application.getVersion(), Matchers.is(VERSION));
        Assert.assertThat(application.getStatus(), Matchers.is(ApplicationStatus.ACTIVE));
        Assert.assertThat(application.getDependencies(), Matchers.empty());
        Assert.assertThat(application.getSetupFile(), Matchers.nullValue());
        Assert.assertThat(application.getConfigs(), Matchers.empty());
        Assert.assertThat(application.getCreated(), Matchers.nullValue());
        Assert.assertThat(application.getDescription(), Matchers.nullValue());
        Assert.assertThat(application.getId(), Matchers.nullValue());
        Assert.assertThat(application.getTags(), Matchers.empty());
        Assert.assertThat(application.getUpdated(), Matchers.nullValue());
    }

    /**
     * Test to make sure we can build an application with all optional parameters.
     */
    @Test
    public void canBuildApplicationWithOptionals() {
        final Application.Builder builder = new Application.Builder(NAME, USER, VERSION, ApplicationStatus.ACTIVE);

        final Set<String> dependencies = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        builder.withDependencies(dependencies);

        final String setupFile = UUID.randomUUID().toString();
        builder.withSetupFile(setupFile);

        final Set<String> configs = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        builder.withConfigs(configs);

        final Date created = new Date();
        builder.withCreated(created);

        final String description = UUID.randomUUID().toString();
        builder.withDescription(description);

        final String id = UUID.randomUUID().toString();
        builder.withId(id);

        final Set<String> tags = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        builder.withTags(tags);

        final Date updated = new Date();
        builder.withUpdated(updated);

        final Application application = builder.build();
        Assert.assertThat(application.getName(), Matchers.is(NAME));
        Assert.assertThat(application.getUser(), Matchers.is(USER));
        Assert.assertThat(application.getVersion(), Matchers.is(VERSION));
        Assert.assertThat(application.getStatus(), Matchers.is(ApplicationStatus.ACTIVE));
        Assert.assertThat(application.getDependencies(), Matchers.is(dependencies));
        Assert.assertThat(application.getSetupFile(), Matchers.is(setupFile));
        Assert.assertThat(application.getConfigs(), Matchers.is(configs));
        Assert.assertThat(application.getCreated(), Matchers.is(created));
        Assert.assertThat(application.getDescription(), Matchers.is(description));
        Assert.assertThat(application.getId(), Matchers.is(id));
        Assert.assertThat(application.getTags(), Matchers.is(tags));
        Assert.assertThat(application.getUpdated(), Matchers.is(updated));
    }

    /**
     * Test to make sure we can build an application with null collection parameters.
     */
    @Test
    public void canBuildApplicationNullOptionals() {
        final Application.Builder builder = new Application.Builder(NAME, USER, VERSION, ApplicationStatus.ACTIVE);
        builder.withDependencies(null);
        builder.withSetupFile(null);
        builder.withConfigs(null);
        builder.withCreated(null);
        builder.withDescription(null);
        builder.withId(null);
        builder.withTags(null);
        builder.withUpdated(null);

        final Application application = builder.build();
        Assert.assertThat(application.getName(), Matchers.is(NAME));
        Assert.assertThat(application.getUser(), Matchers.is(USER));
        Assert.assertThat(application.getVersion(), Matchers.is(VERSION));
        Assert.assertThat(application.getStatus(), Matchers.is(ApplicationStatus.ACTIVE));
        Assert.assertThat(application.getDependencies(), Matchers.empty());
        Assert.assertThat(application.getSetupFile(), Matchers.nullValue());
        Assert.assertThat(application.getConfigs(), Matchers.empty());
        Assert.assertThat(application.getCreated(), Matchers.nullValue());
        Assert.assertThat(application.getDescription(), Matchers.nullValue());
        Assert.assertThat(application.getId(), Matchers.nullValue());
        Assert.assertThat(application.getTags(), Matchers.empty());
        Assert.assertThat(application.getUpdated(), Matchers.nullValue());
    }

    /**
     * Test equals.
     */
    @Test
    public void canFindEquality() {
        final Application.Builder builder = new Application.Builder(NAME, USER, VERSION, ApplicationStatus.ACTIVE);
        builder.withDependencies(null);
        builder.withSetupFile(null);
        builder.withConfigs(null);
        builder.withCreated(null);
        builder.withDescription(null);
        builder.withId(null);
        builder.withTags(null);
        builder.withUpdated(null);
        final Application app1 = builder.build();
        final Application app2 = builder.build();
        builder.withSetupFile(UUID.randomUUID().toString());
        final Application app3 = builder.build();

        Assert.assertTrue(app1.equals(app2));
        Assert.assertTrue(app2.equals(app1));
        Assert.assertFalse(app1.equals(app3));
    }

    /**
     * Test equals.
     */
    @Test
    public void canUseHashCode() {
        final Application.Builder builder = new Application.Builder(NAME, USER, VERSION, null);
        builder.withDependencies(null);
        builder.withSetupFile(null);
        builder.withConfigs(null);
        builder.withCreated(null);
        builder.withDescription(null);
        builder.withId(null);
        builder.withTags(null);
        builder.withUpdated(null);
        final Application app1 = builder.build();
        final Application app2 = builder.build();
        builder.withSetupFile(UUID.randomUUID().toString());
        final Application app3 = builder.build();

        Assert.assertEquals(app1.hashCode(), app2.hashCode());
        Assert.assertNotEquals(app1.hashCode(), app3.hashCode());
    }
}
