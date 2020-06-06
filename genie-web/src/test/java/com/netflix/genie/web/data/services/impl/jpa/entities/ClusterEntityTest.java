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
package com.netflix.genie.web.data.services.impl.jpa.entities;

import com.google.common.collect.Sets;
import com.netflix.genie.common.external.dtos.v4.ClusterStatus;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.validation.ConstraintViolationException;
import java.util.Set;

/**
 * Test the Cluster class.
 *
 * @author tgianos
 */
class ClusterEntityTest extends EntityTestBase {

    private static final String NAME = "h2prod";
    private static final String USER = "tgianos";
    private static final String CONFIG = "s3://netflix/clusters/configs/config1";
    private static final String VERSION = "1.2.3";

    private ClusterEntity c;
    private Set<FileEntity> configs;

    @BeforeEach
    void setup() {
        this.c = new ClusterEntity();
        final FileEntity config = new FileEntity();
        config.setFile(CONFIG);
        this.configs = Sets.newHashSet(config);
        this.c.setName(NAME);
        this.c.setUser(USER);
        this.c.setVersion(VERSION);
        this.c.setStatus(ClusterStatus.UP.name());
    }

    @Test
    void testDefaultConstructor() {
        final ClusterEntity entity = new ClusterEntity();
        Assertions.assertThat(entity.getName()).isNull();
        Assertions.assertThat(entity.getStatus()).isNull();
        Assertions.assertThat(entity.getUser()).isNull();
        Assertions.assertThat(entity.getVersion()).isNull();
        Assertions.assertThat(entity.getConfigs()).isEmpty();
        Assertions.assertThat(entity.getDependencies()).isEmpty();
        Assertions.assertThat(entity.getTags()).isEmpty();
    }

    @Test
    void testValidate() {
        this.validate(this.c);
    }

    @Test
    void testValidateNoName() {
        this.c.setName("");
        Assertions
            .assertThatExceptionOfType(ConstraintViolationException.class)
            .isThrownBy(() -> this.validate(this.c));
    }

    @Test
    void testValidateNoUser() {
        this.c.setUser(" ");
        Assertions
            .assertThatExceptionOfType(ConstraintViolationException.class)
            .isThrownBy(() -> this.validate(this.c));
    }

    @Test
    void testValidateNoVersion() {
        this.c.setVersion("\t");
        Assertions
            .assertThatExceptionOfType(ConstraintViolationException.class)
            .isThrownBy(() -> this.validate(this.c));
    }

    @Test
    void testSetStatus() {
        this.c.setStatus(ClusterStatus.TERMINATED.name());
        Assertions.assertThat(this.c.getStatus()).isEqualTo(ClusterStatus.TERMINATED.name());
    }

    @Test
    void testSetTags() {
        Assertions.assertThat(this.c.getTags()).isEmpty();
        final TagEntity prodTag = new TagEntity();
        prodTag.setTag("prod");
        final TagEntity slaTag = new TagEntity();
        slaTag.setTag("sla");
        final Set<TagEntity> tags = Sets.newHashSet(prodTag, slaTag);
        this.c.setTags(tags);
        Assertions.assertThat(this.c.getTags()).isEqualTo(tags);

        this.c.setTags(null);
        Assertions.assertThat(this.c.getTags()).isEmpty();
    }

    @Test
    void testSetConfigs() {
        Assertions.assertThat(this.c.getConfigs()).isEmpty();
        this.c.setConfigs(this.configs);
        Assertions.assertThat(this.c.getConfigs()).isEqualTo(this.configs);

        this.c.setConfigs(null);
        Assertions.assertThat(c.getConfigs()).isEmpty();
    }

    @Test
    void testSetDependencies() {
        Assertions.assertThat(this.c.getDependencies()).isEmpty();
        final FileEntity dependency = new FileEntity();
        dependency.setFile("s3://netflix/jars/myJar.jar");
        final Set<FileEntity> dependencies = Sets.newHashSet(dependency);
        this.c.setDependencies(dependencies);
        Assertions.assertThat(this.c.getDependencies()).isEqualTo(dependencies);

        this.c.setDependencies(null);
        Assertions.assertThat(this.c.getDependencies()).isEmpty();
    }

    @Test
    void canSetClusterTags() {
        final TagEntity oneTag = new TagEntity();
        oneTag.setTag("one");
        final TagEntity twoTag = new TagEntity();
        twoTag.setTag("tow");
        final TagEntity preTag = new TagEntity();
        preTag.setTag("Pre");
        final Set<TagEntity> tags = Sets.newHashSet(oneTag, twoTag, preTag);
        this.c.setTags(tags);
        Assertions.assertThat(this.c.getTags()).isEqualTo(tags);

        this.c.setTags(Sets.newHashSet());
        Assertions.assertThat(this.c.getTags()).isEmpty();

        this.c.setTags(null);
        Assertions.assertThat(this.c.getTags()).isEmpty();
    }

    @Test
    void testToString() {
        Assertions.assertThat(this.c.toString()).isNotBlank();
    }
}
