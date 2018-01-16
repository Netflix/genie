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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.util.GenieObjectMapper;
import com.netflix.genie.test.categories.UnitTest;
import com.netflix.genie.test.suppliers.RandomSuppliers;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
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
        Assert.assertFalse(application.getType().isPresent());
        Assert.assertFalse(application.getSetupFile().isPresent());
        Assert.assertThat(application.getConfigs(), Matchers.empty());
        Assert.assertFalse(application.getCreated().isPresent());
        Assert.assertFalse(application.getDescription().isPresent());
        Assert.assertFalse(application.getId().isPresent());
        Assert.assertThat(application.getTags(), Matchers.empty());
        Assert.assertFalse(application.getUpdated().isPresent());
        Assert.assertFalse(application.getMetadata().isPresent());
    }

    /**
     * Test to make sure we can build an application with all optional parameters.
     */
    @Test
    public void canBuildApplicationWithOptionals() {
        final Application.Builder builder = new Application.Builder(NAME, USER, VERSION, ApplicationStatus.ACTIVE);

        final Set<String> dependencies = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        builder.withDependencies(dependencies);

        final String type = UUID.randomUUID().toString();
        builder.withType(type);

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
        Assert.assertThat(
            application.getType().orElseGet(RandomSuppliers.STRING),
            Matchers.is(type)
        );
        Assert.assertThat(
            application.getSetupFile().orElseGet(RandomSuppliers.STRING), Matchers.is(setupFile)
        );
        Assert.assertThat(application.getConfigs(), Matchers.is(configs));
        Assert.assertThat(application.getCreated().orElseGet(RandomSuppliers.DATE), Matchers.is(created));
        Assert.assertThat(
            application.getDescription().orElseThrow(IllegalArgumentException::new), Matchers.is(description)
        );
        Assert.assertThat(application.getId().orElseThrow(IllegalArgumentException::new), Matchers.is(id));
        Assert.assertThat(application.getTags(), Matchers.is(tags));
        Assert.assertThat(application.getUpdated().orElseThrow(IllegalArgumentException::new), Matchers.is(updated));
    }

    /**
     * Make sure we can use both builder methods for metadata and that trying to use invalid JSON throws exception.
     *
     * @throws IOException                on JSON error
     * @throws GeniePreconditionException on Invalid JSON
     */
    @Test
    public void canBuildWithMetadata() throws IOException, GeniePreconditionException {
        final ObjectMapper objectMapper = GenieObjectMapper.getMapper();
        final String metadata = "{\"key1\":\"value1\",\"key2\":3}";
        final JsonNode metadataNode = objectMapper.readTree(metadata);
        final Application.Builder builder = new Application.Builder(NAME, USER, VERSION, ApplicationStatus.ACTIVE);
        builder.withMetadata(metadata);
        Assert.assertThat(
            objectMapper.writeValueAsString(builder.build().getMetadata().orElseThrow(IllegalArgumentException::new)),
            Matchers.is(metadata)
        );
        builder.withMetadata(metadataNode);
        Assert.assertThat(
            builder.build().getMetadata().orElseThrow(IllegalArgumentException::new),
            Matchers.is(metadataNode)
        );
        builder.withMetadata((JsonNode) null);
        Assert.assertFalse(builder.build().getMetadata().isPresent());

        try {
            builder.withMetadata("{I'm Not valid Json}");
            Assert.fail();
        } catch (final GeniePreconditionException gpe) {
            Assert.assertTrue(true);
        }
    }

    /**
     * Test to make sure we can build an application with null collection parameters.
     */
    @Test
    public void canBuildApplicationNullOptionals() {
        final Application.Builder builder = new Application.Builder(NAME, USER, VERSION, ApplicationStatus.ACTIVE);
        builder.withDependencies(null);
        builder.withType(null);
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
        Assert.assertFalse(application.getType().isPresent());
        Assert.assertFalse(application.getSetupFile().isPresent());
        Assert.assertThat(application.getConfigs(), Matchers.empty());
        Assert.assertFalse(application.getCreated().isPresent());
        Assert.assertFalse(application.getDescription().isPresent());
        Assert.assertFalse(application.getId().isPresent());
        Assert.assertThat(application.getTags(), Matchers.empty());
        Assert.assertFalse(application.getUpdated().isPresent());
    }

    /**
     * Test equals.
     */
    @Test
    public void canFindEquality() {
        final Application.Builder builder = new Application.Builder(NAME, USER, VERSION, ApplicationStatus.ACTIVE);
        builder.withDependencies(null);
        builder.withType(null);
        builder.withSetupFile(null);
        builder.withConfigs(null);
        builder.withCreated(null);
        builder.withDescription(null);
        builder.withId(UUID.randomUUID().toString());
        builder.withTags(null);
        builder.withUpdated(null);
        final Application app1 = builder.build();
        final Application app2 = builder.build();
        builder.withId(UUID.randomUUID().toString());
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
        builder.withType(null);
        builder.withSetupFile(null);
        builder.withConfigs(null);
        builder.withCreated(null);
        builder.withDescription(null);
        builder.withId(UUID.randomUUID().toString());
        builder.withTags(null);
        builder.withUpdated(null);
        final Application app1 = builder.build();
        final Application app2 = builder.build();
        builder.withId(UUID.randomUUID().toString());
        final Application app3 = builder.build();

        Assert.assertEquals(app1.hashCode(), app2.hashCode());
        Assert.assertNotEquals(app1.hashCode(), app3.hashCode());
    }
}
