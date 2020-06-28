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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.dto.CommandStatus;
import com.netflix.genie.common.internal.exceptions.checked.GenieCheckedException;
import com.netflix.genie.web.exceptions.checked.PreconditionFailedException;
import org.apache.commons.lang3.StringUtils;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import javax.validation.ConstraintViolationException;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Test the {@link CommandEntity} class.
 *
 * @author tgianos
 */
class CommandEntityTest extends EntityTestBase {

    // Comparators needed because the default equality check for CriterionEntity is to compare the id field
    // but that field is set by database code based on insertion and isn't exposed as a setter so substituting
    // using the uniqueId field here
    private static final Comparator<? super List<? extends CriterionEntity>> UNIQUE_ID_CRITERIA_LIST_COMPARATOR =
        (l1, l2) -> {
            if (l1.size() != l2.size()) {
                return l1.size() - l2.size();
            }

            for (int i = 0; i < l1.size(); i++) {
                final String c1 = l1.get(i).getUniqueId().orElse(UUID.randomUUID().toString());
                final String c2 = l2.get(i).getUniqueId().orElse(UUID.randomUUID().toString());
                if (!c1.equals(c2)) {
                    return c1.compareTo(c2);
                }
            }
            return 0;
        };
    private static final Comparator<CriterionEntity> UNIQUE_ID_CRITERION_COMPARATOR = Comparator.comparing(
        criterion -> criterion.getUniqueId().orElse(UUID.randomUUID().toString())
    );
    private static final String NAME = "pig13";
    private static final String USER = "tgianos";
    private static final List<String> EXECUTABLE = Lists.newArrayList("/bin/pig13", "-Dblah");
    private static final String VERSION = "1.0";
    private static final long CHECK_DELAY = 18083L;
    private static final int MEMORY = 10_240;

    private CommandEntity c;

    @BeforeEach
    void setup() {
        this.c = new CommandEntity();
        this.c.setName(NAME);
        this.c.setUser(USER);
        this.c.setVersion(VERSION);
        this.c.setStatus(CommandStatus.ACTIVE.name());
        this.c.setExecutable(EXECUTABLE);
        this.c.setCheckDelay(CHECK_DELAY);
        this.c.setMemory(MEMORY);
    }

    @Test
    void testDefaultConstructor() {
        final CommandEntity entity = new CommandEntity();
        Assertions.assertThat(entity.getSetupFile()).isNotPresent();
        Assertions.assertThat(entity.getExecutable()).isEmpty();
        Assertions.assertThat(entity.getCheckDelay()).isEqualTo(Command.DEFAULT_CHECK_DELAY);
        Assertions.assertThat(entity.getName()).isNull();
        Assertions.assertThat(entity.getStatus()).isNull();
        Assertions.assertThat(entity.getUser()).isNull();
        Assertions.assertThat(entity.getVersion()).isNull();
        Assertions.assertThat(entity.getConfigs()).isEmpty();
        Assertions.assertThat(entity.getDependencies()).isEmpty();
        Assertions.assertThat(entity.getTags()).isEmpty();
        Assertions.assertThat(entity.getApplications()).isEmpty();
        Assertions.assertThat(entity.getMemory()).isNotPresent();
        Assertions.assertThat(entity.getLauncherExt()).isNotPresent();
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
        this.c.setUser("   ");
        Assertions
            .assertThatExceptionOfType(ConstraintViolationException.class)
            .isThrownBy(() -> this.validate(this.c));
    }

    @Test
    void testValidateNoVersion() {
        this.c.setVersion("");
        Assertions
            .assertThatExceptionOfType(ConstraintViolationException.class)
            .isThrownBy(() -> this.validate(this.c));
    }

    @Test
    void testValidateEmptyExecutable() {
        this.c.setExecutable(Lists.newArrayList());
        Assertions
            .assertThatExceptionOfType(ConstraintViolationException.class)
            .isThrownBy(() -> this.validate(this.c));
    }

    @Test
    void testValidateBlankExecutable() {
        this.c.setExecutable(Lists.newArrayList("    "));
        Assertions
            .assertThatExceptionOfType(ConstraintViolationException.class)
            .isThrownBy(() -> this.validate(this.c));
    }

    @Test
    void testValidateExecutableArgumentTooLong() {
        this.c.setExecutable(Lists.newArrayList(StringUtils.leftPad("", 1025, 'e')));
        Assertions
            .assertThatExceptionOfType(ConstraintViolationException.class)
            .isThrownBy(() -> this.validate(this.c));
    }

    @Test
    void testValidateBadCheckDelay() {
        this.c.setCheckDelay(0L);
        Assertions
            .assertThatExceptionOfType(ConstraintViolationException.class)
            .isThrownBy(() -> this.validate(this.c));
    }

    @Test
    void testSetStatus() {
        this.c.setStatus(CommandStatus.ACTIVE.name());
        Assertions.assertThat(this.c.getStatus()).isEqualTo(CommandStatus.ACTIVE.name());
    }

    @Test
    void testSetSetupFile() {
        Assertions.assertThat(this.c.getSetupFile()).isNotPresent();
        final String setupFile = "s3://netflix.propFile";
        final FileEntity setupFileEntity = new FileEntity(setupFile);
        this.c.setSetupFile(setupFileEntity);
        Assertions.assertThat(this.c.getSetupFile()).isPresent().contains(setupFileEntity);
    }

    @Test
    void testSetExecutable() {
        this.c.setExecutable(EXECUTABLE);
        Assertions.assertThat(this.c.getExecutable()).isEqualTo(EXECUTABLE);
    }

    @Test
    void testSetCheckDelay() {
        final long newDelay = 108327L;
        Assertions.assertThat(this.c.getCheckDelay()).isEqualTo(CHECK_DELAY);
        this.c.setCheckDelay(newDelay);
        Assertions.assertThat(this.c.getCheckDelay()).isEqualTo(newDelay);
    }

    @Test
    void testSetMemory() {
        Assertions.assertThat(this.c.getMemory()).isPresent().contains(MEMORY);
        final int newMemory = MEMORY + 1;
        this.c.setMemory(newMemory);
        Assertions.assertThat(this.c.getMemory()).isPresent().contains(newMemory);
    }

    @Test
    void testSetConfigs() {
        Assertions.assertThat(this.c.getConfigs()).isEmpty();
        final Set<FileEntity> configs = Sets.newHashSet(new FileEntity("s3://netflix.configFile"));
        this.c.setConfigs(configs);
        Assertions.assertThat(this.c.getConfigs()).isEqualTo(configs);

        this.c.setConfigs(null);
        Assertions.assertThat(this.c.getConfigs()).isEmpty();
    }

    @Test
    void testSetDependencies() {
        Assertions.assertThat(this.c.getDependencies()).isEmpty();
        final Set<FileEntity> dependencies = Sets.newHashSet(new FileEntity("dep1"));
        this.c.setDependencies(dependencies);
        Assertions.assertThat(this.c.getDependencies()).isEqualTo(dependencies);

        this.c.setDependencies(null);
        Assertions.assertThat(this.c.getDependencies()).isEmpty();
    }

    @Test
    void testSetTags() {
        Assertions.assertThat(this.c.getTags()).isEmpty();
        final TagEntity one = new TagEntity("tag1");
        final TagEntity two = new TagEntity("tag2");
        final Set<TagEntity> tags = Sets.newHashSet(one, two);
        this.c.setTags(tags);
        Assertions.assertThat(this.c.getTags()).isEqualTo(tags);

        this.c.setTags(null);
        Assertions.assertThat(this.c.getTags()).isEmpty();
    }

    @Test
    void testSetApplications() throws GenieCheckedException {
        Assertions.assertThat(this.c.getApplications()).isEmpty();
        final ApplicationEntity one = new ApplicationEntity();
        one.setUniqueId("one");
        final ApplicationEntity two = new ApplicationEntity();
        two.setUniqueId("two");
        final List<ApplicationEntity> applicationEntities = Lists.newArrayList(one, two);
        this.c.setApplications(applicationEntities);
        Assertions.assertThat(this.c.getApplications()).isEqualTo(applicationEntities);
        Assertions.assertThat(one.getCommands()).contains(this.c);
        Assertions.assertThat(two.getCommands()).contains(this.c);

        applicationEntities.clear();
        applicationEntities.add(two);
        this.c.setApplications(applicationEntities);
        Assertions.assertThat(this.c.getApplications()).isEqualTo(applicationEntities);
        Assertions.assertThat(one.getCommands()).doesNotContain(this.c);
        Assertions.assertThat(two.getCommands()).contains(this.c);
        this.c.setApplications(null);
        Assertions.assertThat(this.c.getApplications()).isEmpty();
        Assertions.assertThat(one.getCommands()).isEmpty();
        Assertions.assertThat(two.getCommands()).isEmpty();
    }

    @Test
    void cantSetApplicationsIfDuplicates() {
        final ApplicationEntity one = Mockito.mock(ApplicationEntity.class);
        Mockito.when(one.getUniqueId()).thenReturn(UUID.randomUUID().toString());
        final ApplicationEntity two = Mockito.mock(ApplicationEntity.class);
        Mockito.when(two.getUniqueId()).thenReturn(UUID.randomUUID().toString());

        Assertions
            .assertThatExceptionOfType(PreconditionFailedException.class)
            .isThrownBy(() -> this.c.setApplications(Lists.newArrayList(one, two, one)));
    }

    @Test
    void canAddApplication() throws PreconditionFailedException {
        final String id = UUID.randomUUID().toString();
        final ApplicationEntity app = new ApplicationEntity();
        app.setUniqueId(id);

        this.c.addApplication(app);
        Assertions.assertThat(this.c.getApplications()).contains(app);
        Assertions.assertThat(app.getCommands()).contains(this.c);
    }

    @Test
    void cantAddApplicationThatAlreadyIsInList() throws PreconditionFailedException {
        final String id = UUID.randomUUID().toString();
        final ApplicationEntity app = new ApplicationEntity();
        app.setUniqueId(id);

        this.c.addApplication(app);
        Assertions
            .assertThatExceptionOfType(PreconditionFailedException.class)
            .isThrownBy(() -> this.c.addApplication(app));
    }

    @Test
    void canRemoveApplication() throws PreconditionFailedException {
        final ApplicationEntity one = new ApplicationEntity();
        one.setUniqueId("one");
        final ApplicationEntity two = new ApplicationEntity();
        two.setUniqueId("two");
        Assertions.assertThat(this.c.getApplications()).isEmpty();
        this.c.addApplication(one);
        Assertions.assertThat(this.c.getApplications()).contains(one);
        Assertions.assertThat(this.c.getApplications()).doesNotContain(two);
        Assertions.assertThat(one.getCommands()).contains(this.c);
        Assertions.assertThat(two.getCommands()).isEmpty();
        this.c.addApplication(two);
        Assertions.assertThat(this.c.getApplications()).contains(one);
        Assertions.assertThat(this.c.getApplications()).contains(two);
        Assertions.assertThat(one.getCommands()).contains(this.c);
        Assertions.assertThat(two.getCommands()).contains(this.c);

        this.c.removeApplication(one);
        Assertions.assertThat(this.c.getApplications()).doesNotContain(one);
        Assertions.assertThat(this.c.getApplications()).contains(two);
        Assertions.assertThat(one.getCommands()).doesNotContain(this.c);
        Assertions.assertThat(two.getCommands()).contains(this.c);
    }

    @Test
    void testToString() {
        Assertions.assertThat(this.c.toString()).isNotBlank();
    }

    @Test
    void canManipulateClusterCriteria() {
        Assertions.assertThat(this.c.getClusterCriteria()).isEmpty();
        final CriterionEntity criterion0 = this.createTestCriterion();
        final CriterionEntity criterion1 = this.createTestCriterion();
        final CriterionEntity criterion2 = this.createTestCriterion();
        final CriterionEntity criterion3 = this.createTestCriterion();
        final CriterionEntity criterion4 = this.createTestCriterion();
        final CriterionEntity criterion5 = this.createTestCriterion();
        final CriterionEntity criterion6 = this.createTestCriterion();
        final CriterionEntity criterion7 = this.createTestCriterion();
        final CriterionEntity criterion8 = this.createTestCriterion();
        final List<CriterionEntity> clusterCriteria = Lists.newArrayList(criterion0, criterion1, criterion2);
        this.c.setClusterCriteria(clusterCriteria);
        Assertions
            .assertThat(this.c.getClusterCriteria())
            .usingComparator(UNIQUE_ID_CRITERIA_LIST_COMPARATOR)
            .isEqualTo(clusterCriteria);
        this.c.setClusterCriteria(null);
        Assertions.assertThat(this.c.getClusterCriteria()).isEmpty();
        this.c.setClusterCriteria(clusterCriteria);
        Assertions
            .assertThat(this.c.getClusterCriteria())
            .usingComparator(UNIQUE_ID_CRITERIA_LIST_COMPARATOR)
            .isEqualTo(clusterCriteria);
        this.c.setClusterCriteria(Lists.newArrayList());
        Assertions.assertThat(this.c.getClusterCriteria()).isEmpty();
        this.c.setClusterCriteria(clusterCriteria);
        Assertions
            .assertThat(this.c.getClusterCriteria())
            .usingComparator(UNIQUE_ID_CRITERIA_LIST_COMPARATOR)
            .isEqualTo(clusterCriteria);
        this.c.addClusterCriterion(criterion3);
        Assertions
            .assertThat(this.c.getClusterCriteria())
            .size()
            .isEqualTo(4)
            .returnToIterable()
            .element(3)
            .usingComparator(UNIQUE_ID_CRITERION_COMPARATOR)
            .isEqualTo(criterion3);
        this.c.addClusterCriterion(criterion4, this.c.getClusterCriteria().size());
        Assertions
            .assertThat(this.c.getClusterCriteria())
            .size()
            .isEqualTo(5)
            .returnToIterable()
            .element(4)
            .usingComparator(UNIQUE_ID_CRITERION_COMPARATOR)
            .isEqualTo(criterion4);
        this.c.addClusterCriterion(criterion5, this.c.getClusterCriteria().size() + 80);
        Assertions
            .assertThat(this.c.getClusterCriteria())
            .size()
            .isEqualTo(6)
            .returnToIterable()
            .element(5)
            .usingComparator(UNIQUE_ID_CRITERION_COMPARATOR)
            .isEqualTo(criterion5);
        this.c.addClusterCriterion(criterion6, -5000);
        Assertions
            .assertThat(this.c.getClusterCriteria())
            .size()
            .isEqualTo(7)
            .returnToIterable()
            .element(0)
            .usingComparator(UNIQUE_ID_CRITERION_COMPARATOR)
            .isEqualTo(criterion6);
        this.c.addClusterCriterion(criterion7, 0);
        Assertions
            .assertThat(this.c.getClusterCriteria())
            .size()
            .isEqualTo(8)
            .returnToIterable()
            .element(0)
            .usingComparator(UNIQUE_ID_CRITERION_COMPARATOR)
            .isEqualTo(criterion7);
        this.c.addClusterCriterion(criterion8, 1);
        Assertions
            .assertThat(this.c.getClusterCriteria())
            .size()
            .isEqualTo(9)
            .returnToIterable()
            .element(1)
            .usingComparator(UNIQUE_ID_CRITERION_COMPARATOR)
            .isEqualTo(criterion8);

        Assertions
            .assertThatIllegalArgumentException()
            .isThrownBy(() -> this.c.removeClusterCriterion(-1));
        Assertions
            .assertThatIllegalArgumentException()
            .isThrownBy(() -> this.c.removeClusterCriterion(this.c.getClusterCriteria().size()));
        Assertions
            .assertThat(this.c.removeClusterCriterion(0))
            .usingComparator(UNIQUE_ID_CRITERION_COMPARATOR)
            .isEqualTo(criterion7);
        Assertions
            .assertThat(this.c.removeClusterCriterion(0))
            .usingComparator(UNIQUE_ID_CRITERION_COMPARATOR)
            .isEqualTo(criterion8);
        Assertions
            .assertThat(this.c.removeClusterCriterion(0))
            .usingComparator(UNIQUE_ID_CRITERION_COMPARATOR)
            .isEqualTo(criterion6);
        Assertions
            .assertThat(this.c.removeClusterCriterion(3))
            .usingComparator(UNIQUE_ID_CRITERION_COMPARATOR)
            .isEqualTo(criterion3);
        Assertions
            .assertThat(this.c.removeClusterCriterion(4))
            .usingComparator(UNIQUE_ID_CRITERION_COMPARATOR)
            .isEqualTo(criterion5);
        Assertions
            .assertThat(this.c.removeClusterCriterion(3))
            .usingComparator(UNIQUE_ID_CRITERION_COMPARATOR)
            .isEqualTo(criterion4);
        Assertions
            .assertThat(this.c.getClusterCriteria())
            .usingComparator(UNIQUE_ID_CRITERIA_LIST_COMPARATOR)
            .usingComparator(UNIQUE_ID_CRITERIA_LIST_COMPARATOR)
            .isEqualTo(clusterCriteria);
    }

    @Test
    void canSetLauncherExt() {
        Assertions.assertThat(this.c.getLauncherExt()).isNotPresent();
        final JsonNode launcherMetadata = Mockito.mock(JsonNode.class);
        this.c.setLauncherExt(launcherMetadata);
        Assertions.assertThat(this.c.getLauncherExt()).isPresent().contains(launcherMetadata);
    }

    private CriterionEntity createTestCriterion() {
        final CriterionEntity criterion = new CriterionEntity();
        criterion.setUniqueId(UUID.randomUUID().toString());
        return criterion;
    }
}
