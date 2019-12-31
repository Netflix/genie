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
import com.netflix.genie.common.external.util.GenieObjectMapper;
import com.netflix.genie.test.suppliers.RandomSuppliers;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.Set;
import java.util.UUID;

/**
 * Tests for the Application DTO.
 *
 * @author tgianos
 * @since 3.0.0
 */
class ApplicationTest {

    private static final String NAME = UUID.randomUUID().toString();
    private static final String USER = UUID.randomUUID().toString();
    private static final String VERSION = UUID.randomUUID().toString();

    /**
     * Test to make sure we can build an application using the default builder constructor.
     */
    @Test
    void canBuildApplication() {
        final Application application = new Application.Builder(NAME, USER, VERSION, ApplicationStatus.ACTIVE).build();
        Assertions.assertThat(application.getName()).isEqualTo(NAME);
        Assertions.assertThat(application.getUser()).isEqualTo(USER);
        Assertions.assertThat(application.getVersion()).isEqualTo(VERSION);
        Assertions.assertThat(application.getStatus()).isEqualTo(ApplicationStatus.ACTIVE);
        Assertions.assertThat(application.getDependencies()).isEmpty();
        Assertions.assertThat(application.getType().isPresent()).isFalse();
        Assertions.assertThat(application.getSetupFile().isPresent()).isFalse();
        Assertions.assertThat(application.getConfigs()).isEmpty();
        Assertions.assertThat(application.getCreated().isPresent()).isFalse();
        Assertions.assertThat(application.getDescription().isPresent()).isFalse();
        Assertions.assertThat(application.getId().isPresent()).isFalse();
        Assertions.assertThat(application.getTags()).isEmpty();
        Assertions.assertThat(application.getUpdated().isPresent()).isFalse();
        Assertions.assertThat(application.getMetadata().isPresent()).isFalse();
    }

    /**
     * Test to make sure we can build an application with all optional parameters.
     */
    @Test
    void canBuildApplicationWithOptionals() {
        final Application.Builder builder = new Application.Builder(NAME, USER, VERSION, ApplicationStatus.ACTIVE);

        final Set<String> dependencies = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        builder.withDependencies(dependencies);

        final String type = UUID.randomUUID().toString();
        builder.withType(type);

        final String setupFile = UUID.randomUUID().toString();
        builder.withSetupFile(setupFile);

        final Set<String> configs = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        builder.withConfigs(configs);

        final Instant created = Instant.now();
        builder.withCreated(created);

        final String description = UUID.randomUUID().toString();
        builder.withDescription(description);

        final String id = UUID.randomUUID().toString();
        builder.withId(id);

        final Set<String> tags = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        builder.withTags(tags);

        final Instant updated = Instant.now();
        builder.withUpdated(updated);

        final Application application = builder.build();
        Assertions.assertThat(application.getName()).isEqualTo(NAME);
        Assertions.assertThat(application.getUser()).isEqualTo(USER);
        Assertions.assertThat(application.getVersion()).isEqualTo(VERSION);
        Assertions.assertThat(application.getStatus()).isEqualTo(ApplicationStatus.ACTIVE);
        Assertions.assertThat(application.getDependencies()).isEqualTo(dependencies);
        Assertions.assertThat(application.getType().orElseGet(RandomSuppliers.STRING)).isEqualTo(type);
        Assertions.assertThat(application.getSetupFile().orElseGet(RandomSuppliers.STRING)).isEqualTo(setupFile);
        Assertions.assertThat(application.getConfigs()).isEqualTo(configs);
        Assertions.assertThat(application.getCreated().orElseGet(RandomSuppliers.INSTANT)).isEqualTo(created);
        Assertions
            .assertThat(application.getDescription().orElseThrow(IllegalArgumentException::new))
            .isEqualTo(description);
        Assertions
            .assertThat(application.getId().orElseThrow(IllegalArgumentException::new))
            .isEqualTo(id);
        Assertions
            .assertThat(application.getTags())
            .isEqualTo(tags);
        Assertions
            .assertThat(application.getUpdated().orElseThrow(IllegalArgumentException::new))
            .isEqualTo(updated);
    }

    /**
     * Make sure we can use both builder methods for metadata and that trying to use invalid JSON throws exception.
     *
     * @throws IOException                on JSON error
     * @throws GeniePreconditionException on Invalid JSON
     */
    @Test
    void canBuildWithMetadata() throws IOException, GeniePreconditionException {
        final ObjectMapper objectMapper = GenieObjectMapper.getMapper();
        final String metadata = "{\"key1\":\"value1\",\"key2\":3}";
        final JsonNode metadataNode = objectMapper.readTree(metadata);
        final Application.Builder builder = new Application.Builder(NAME, USER, VERSION, ApplicationStatus.ACTIVE);
        builder.withMetadata(metadata);
        Assertions
            .assertThat(
                objectMapper.writeValueAsString(
                    builder.build().getMetadata().orElseThrow(IllegalArgumentException::new)
                )
            )
            .isEqualTo(metadata);
        builder.withMetadata(metadataNode);
        Assertions
            .assertThat(builder.build().getMetadata().orElseThrow(IllegalArgumentException::new))
            .isEqualTo(metadataNode);
        builder.withMetadata((JsonNode) null);
        Assertions.assertThat(builder.build().getMetadata().isPresent()).isFalse();
        Assertions
            .assertThatExceptionOfType(GeniePreconditionException.class)
            .isThrownBy(() -> builder.withMetadata("{I'm Not valid Json}"));
    }

    /**
     * Test to make sure we can build an application with null collection parameters.
     */
    @Test
    void canBuildApplicationNullOptionals() {
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
        Assertions.assertThat(application.getName()).isEqualTo(NAME);
        Assertions.assertThat(application.getUser()).isEqualTo(USER);
        Assertions.assertThat(application.getVersion()).isEqualTo(VERSION);
        Assertions.assertThat(application.getStatus()).isEqualTo(ApplicationStatus.ACTIVE);
        Assertions.assertThat(application.getDependencies()).isEmpty();
        Assertions.assertThat(application.getType().isPresent()).isFalse();
        Assertions.assertThat(application.getSetupFile().isPresent()).isFalse();
        Assertions.assertThat(application.getConfigs()).isEmpty();
        Assertions.assertThat(application.getCreated().isPresent()).isFalse();
        Assertions.assertThat(application.getDescription().isPresent()).isFalse();
        Assertions.assertThat(application.getId().isPresent()).isFalse();
        Assertions.assertThat(application.getTags()).isEmpty();
        Assertions.assertThat(application.getUpdated().isPresent()).isFalse();
    }

    /**
     * Test equals.
     */
    @Test
    void testEqualityAndHashCode() {
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

        Assertions.assertThat(app1).isEqualTo(app2);
        Assertions.assertThat(app1).isNotEqualTo(app3);

        Assertions.assertThat(app1.hashCode()).isEqualTo(app2.hashCode());
        Assertions.assertThat(app1.hashCode()).isNotEqualTo(app3.hashCode());
    }
}
