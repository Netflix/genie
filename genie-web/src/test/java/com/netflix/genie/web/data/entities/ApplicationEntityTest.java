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
package com.netflix.genie.web.data.entities;

import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.ApplicationStatus;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.validation.ConstraintViolationException;
import java.util.Set;

/**
 * Test the Application class.
 *
 * @author tgianos
 */
class ApplicationEntityTest extends EntityTestBase {
    private static final String NAME = "pig";
    private static final String USER = "tgianos";
    private static final String VERSION = "1.0";

    private ApplicationEntity a;

    /**
     * Setup the tests.
     */
    @BeforeEach
    void setup() {
        this.a = new ApplicationEntity();
        this.a.setName(NAME);
        this.a.setUser(USER);
        this.a.setVersion(VERSION);
        this.a.setStatus(ApplicationStatus.ACTIVE.name());
    }

    /**
     * Test the default Constructor.
     */
    @Test
    void testDefaultConstructor() {
        final ApplicationEntity entity = new ApplicationEntity();
        Assertions.assertThat(entity.getSetupFile()).isNotPresent();
        Assertions.assertThat(entity.getStatus()).isNull();
        Assertions.assertThat(entity.getName()).isNull();
        Assertions.assertThat(entity.getUser()).isNull();
        Assertions.assertThat(entity.getVersion()).isNull();
        Assertions.assertThat(entity.getDependencies()).isEmpty();
        Assertions.assertThat(entity.getConfigs()).isEmpty();
        Assertions.assertThat(entity.getTags()).isEmpty();
        Assertions.assertThat(entity.getCommands()).isEmpty();
    }

    /**
     * Make sure validation works on valid apps.
     */
    @Test
    void testValidate() {
        this.a.setName(NAME);
        this.a.setUser(USER);
        this.a.setVersion(VERSION);
        this.a.setStatus(ApplicationStatus.ACTIVE.name());
        this.validate(this.a);
    }

    /**
     * Make sure validation works on with failure from super class.
     */
    @Test
    void testValidateNoName() {
        this.a.setName("");
        Assertions
            .assertThatExceptionOfType(ConstraintViolationException.class)
            .isThrownBy(() -> this.validate(this.a));
    }

    /**
     * Make sure validation works on with failure from super class.
     */
    @Test
    void testValidateNoUser() {
        this.a.setUser("");
        Assertions
            .assertThatExceptionOfType(ConstraintViolationException.class)
            .isThrownBy(() -> this.validate(this.a));
    }

    /**
     * Make sure validation works on with failure from super class.
     */
    @Test
    void testValidateNoVersion() {
        this.a.setVersion("");
        Assertions
            .assertThatExceptionOfType(ConstraintViolationException.class)
            .isThrownBy(() -> this.validate(this.a));
    }

    /**
     * Test setting the status.
     */
    @Test
    void testSetStatus() {
        this.a.setStatus(ApplicationStatus.ACTIVE.name());
        Assertions.assertThat(this.a.getStatus()).isEqualTo(ApplicationStatus.ACTIVE.name());
    }

    /**
     * Test setting the setup file.
     */
    @Test
    void testSetSetupFile() {
        Assertions.assertThat(this.a.getSetupFile()).isNotPresent();
        final FileEntity setupFileEntity = new FileEntity("s3://netflix.propFile");
        this.a.setSetupFile(setupFileEntity);
        Assertions.assertThat(this.a.getSetupFile()).isPresent().contains(setupFileEntity);
    }

    /**
     * Test setting the configs.
     */
    @Test
    void testSetConfigs() {
        final Set<FileEntity> configs = Sets.newHashSet(new FileEntity("s3://netflix.configFile"));
        this.a.setConfigs(configs);
        Assertions.assertThat(this.a.getConfigs()).isEqualTo(configs);

        this.a.setConfigs(null);
        Assertions.assertThat(this.a.getConfigs()).isEmpty();
    }

    /**
     * Test setting the jars.
     */
    @Test
    void testSetDependencies() {
        final Set<FileEntity> dependencies = Sets.newHashSet(new FileEntity("s3://netflix/jars/myJar.jar"));
        this.a.setDependencies(dependencies);
        Assertions.assertThat(this.a.getDependencies()).isEqualTo(dependencies);

        this.a.setDependencies(null);
        Assertions.assertThat(this.a.getDependencies()).isEmpty();
    }

    /**
     * Test setting the tags.
     */
    @Test
    void testSetTags() {
        final TagEntity tag1 = new TagEntity("tag1");
        final TagEntity tag2 = new TagEntity("tag2");
        final Set<TagEntity> tags = Sets.newHashSet(tag1, tag2);

        this.a.setTags(tags);
        Assertions.assertThat(this.a.getTags()).isEqualTo(tags);

        this.a.setTags(null);
        Assertions.assertThat(this.a.getTags()).isEmpty();
    }

    /**
     * Test setting the commands.
     */
    @Test
    void testSetCommands() {
        final Set<CommandEntity> commandEntities = Sets.newHashSet(new CommandEntity());
        this.a.setCommands(commandEntities);
        Assertions.assertThat(this.a.getCommands()).isEqualTo(commandEntities);

        this.a.setCommands(null);
        Assertions.assertThat(this.a.getCommands()).isEmpty();
    }

    /**
     * Test the toString method.
     */
    @Test
    void testToString() {
        Assertions.assertThat(this.a.toString()).isNotBlank();
    }
}
